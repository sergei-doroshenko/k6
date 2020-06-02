/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2019 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package executor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/loadimpact/k6/lib"
)

// This is a helper type used in executors where we have to dynamically control
// the number of VUs that are simultaneously running. For the moment, it is used
// in the VariableLoopingVUs and the ExternallyControlled executors.
//
// TODO: something simpler?
type vuHandle struct {
	mutex     *sync.RWMutex
	parentCtx context.Context
	getVU     func() (lib.InitializedVU, error)
	returnVU  func(lib.InitializedVU)
	config    *BaseConfig

	vu           lib.InitializedVU
	canStartIter chan struct{}
	// This is here only to signal that something has changed it must be added to and read with atomics
	// and helps to skip checking all the contexts and channels all the time
	change int32

	ctx    context.Context
	cancel func()
	logger *logrus.Entry
}

func newStoppedVUHandle(
	parentCtx context.Context, getVU func() (lib.InitializedVU, error),
	returnVU func(lib.InitializedVU), config *BaseConfig, logger *logrus.Entry,
) *vuHandle {
	lock := &sync.RWMutex{}
	ctx, cancel := context.WithCancel(parentCtx)

	vh := &vuHandle{
		mutex:     lock,
		parentCtx: parentCtx,
		getVU:     getVU,
		config:    config,

		canStartIter: make(chan struct{}),
		change:       1,

		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}

	vh.returnVU = func(v lib.InitializedVU) {
		// Don't return the initialized VU back
		vh.mutex.Lock()
		select {
		case <-vh.parentCtx.Done():
			// we are done just ruturn the VU
			vh.vu = nil
			atomic.StoreInt32(&vh.change, 1)
			vh.mutex.Unlock()
			returnVU(v)
		default:
			select {
			case <-vh.canStartIter:
				vh.mutex.Unlock()
				// we can continue with itearting - lets not return the vu
			default:
				vh.vu = nil
				atomic.StoreInt32(&vh.change, 1)
				vh.mutex.Unlock()
				returnVU(v)
			}
		}
	}

	return vh
}

func (vh *vuHandle) start() (err error) {
	vh.mutex.Lock()
	vh.logger.Debug("Start")
	if vh.vu == nil {
		vh.vu, err = vh.getVU()
		if err != nil {
			return err
		}
		atomic.AddInt32(&vh.change, 1)
	}
	close(vh.canStartIter)
	vh.mutex.Unlock()
	return nil
}

func (vh *vuHandle) gracefulStop() {
	vh.mutex.Lock()
	select {
	case <-vh.canStartIter:
		atomic.AddInt32(&vh.change, 1)
		vh.canStartIter = make(chan struct{})
		vh.logger.Debug("Graceful stop")
	default:
		// do nothing, the signalling channel was already initialized by hardStop()
	}
	vh.mutex.Unlock()
}

func (vh *vuHandle) hardStop() {
	vh.mutex.Lock()
	vh.logger.Debug("Hard stop")
	vh.cancel() // cancel the previous context
	atomic.AddInt32(&vh.change, 1)
	vh.ctx, vh.cancel = context.WithCancel(vh.parentCtx) // create a new context
	select {
	case <-vh.canStartIter:
		vh.canStartIter = make(chan struct{})
	default:
		// do nothing, the signalling channel was already initialized by gracefulStop()
	}
	vh.mutex.Unlock()
}

//TODO: simplify this somehow - I feel like there should be a better way to
//implement this logic... maybe with sync.Cond?
func (vh *vuHandle) runLoopsIfPossible(runIter func(context.Context, lib.ActiveVU)) {
	// We can probably initialize here, but it's also easier to just use the slow path in the second
	// part of the for loop
	var (
		executorDone = vh.parentCtx.Done()
		ctx          context.Context
		cancel       func()
		canStartIter chan struct{}
		vu           lib.ActiveVU
	)

	for {
		ch := atomic.LoadInt32(&vh.change)
		if ch == 0 { // fast path
			runIter(ctx, vu)
			continue
		}
		// slow path - something has changed - get what and wait until we can do more iterations
		if cancel != nil {
			cancel() // signal to return the vu before we continue
		}
		vh.mutex.RLock()
		canStartIter, ctx = vh.canStartIter, vh.ctx
		vh.mutex.RUnlock()

		select {
		case <-executorDone:
			// The whole executor is done, nothing more to do.
			return
		default:
			// We're not running, but the executor isn't done yet, so we wait
			// for either one of those conditions.
			select {
			case <-canStartIter:
				// reinitialize
				vh.mutex.RLock()
				ctx = vh.ctx
				initVU := vh.vu
				atomic.StoreInt32(&vh.change, 0) // clear changes here
				vh.mutex.RUnlock()

				// get a cancel so we can return VU when we get's gracefully stopped
				ctx, cancel = context.WithCancel(ctx)
				vu = initVU.Activate(getVUActivationParams(ctx, *vh.config, vh.returnVU))
			case <-ctx.Done():
				// hardStop was called, start a fresh iteration to get the new
				// context and signal channel
			case <-executorDone:
				// The whole executor is done, nothing more to do.
				return
			}
		}
	}
}
