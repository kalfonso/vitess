/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"context"
	"flag"
	"math/rand"
	"time"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	tmc                = tmclient.NewTabletManagerClient()
	masterCheckFreq    = flag.Duration("master-check-millis", 1*time.Second, "frequency to check master status")
	masterCheckJitter  = flag.Duration("master-check-jitter", 500*time.Millisecond, "jitter to add to the master check interval")
	masterCheckTimeout = flag.Duration("master_check_timeout", 5*time.Second, "master check timeout")
)

type masterRepairJob struct {
	agent     *ActionAgent
	done      chan bool
}

func startNewMasterRepairJob(ts *topo.Server, agent *ActionAgent) {
	done := make(chan bool)
	job := &masterRepairJob{
		agent:     agent,
		done:      done,
	}

	job.start()
	servenv.OnClose(func(){
		job.stop()
	})
}

func (job *masterRepairJob) start() {
	go func() {
		for {
			time.Sleep(job.calculateDelay())

			select {
			case <-job.done:
				return
			default:
				job.checkMaster()
			}
		}
	}()
}

func (job *masterRepairJob) stop() {
	job.done <- true
}

func (job *masterRepairJob) checkMaster() {
	ctx, cancel := context.WithTimeout(context.Background(), *masterCheckTimeout)
	defer cancel()

	agent := job.agent
	tablet := agent.Tablet()
	ks := tablet.Keyspace
	shard := tablet.Shard
	alias := tablet.Alias
	shardInfo, err := job.agent.TopoServer.GetShard(ctx, ks, shard)
	if err != nil {
		log.Error("could not retrieve shard info for keyspace: %s, shard: %s", ks, shard)
		return
	}

	if tablet.Type != topodata.TabletType_MASTER {
		if shardInfo.GetMasterAlias() == nil {
			// No master in this shard, attempt to become master
			if err := agent.TabletExternallyReparented(ctx, alias.String()); err != nil {
				log.Errorf("could not become master tablet: %v, error: %v", tablet, err)
			}
			return
		}

		if topoproto.TabletAliasEqual(shardInfo.GetMasterAlias(), alias) {
			// we're master in topology, refresh state
			if err := agent.RefreshState(ctx); err != nil {
				log.Errorf("could not refresh state tablet: %v, error: %v", tablet, err)
			}
			return
		}

		// this tablet is not the master. Run healthcheck against master and attempt to become master if it fails
		masterInfo, err := agent.TopoServer.GetTablet(ctx, shardInfo.MasterAlias)
		if err != nil {
			log.Errorf("could not retrieve tablet info for master: %v, error: %v", shardInfo.MasterAlias, err)
			return
		}

		// Healthcheck master and TER to self on failure
		hcCtx, hcCancel := context.WithTimeout(ctx, *masterCheckTimeout/2)
		defer hcCancel()
		if err := tmc.RunHealthCheck(hcCtx, masterInfo.Tablet); err != nil {
			log.Errorf("failed to run explicit healthcheck on tablet: %v err: %v", masterInfo, err)
			if err := agent.TabletExternallyReparented(ctx, alias.String()); err != nil {
				log.Errorf("could not become master tablet: %v, error: %v", tablet, err)
			}
			return
		}
	}

	// this tablet is the master, reconcile with the topology if required
	if !topoproto.TabletAliasEqual(shardInfo.GetMasterAlias(), alias) {
		if err := agent.RefreshState(ctx); err != nil {
			log.Errorf("could not refresh state tablet: %v, error: %v", tablet, err)
		}
		return
	}
}

func (job *masterRepairJob) calculateDelay() time.Duration {
	jitter := rand.Int63n(masterCheckJitter.Nanoseconds())
	return time.Duration(masterCheckFreq.Nanoseconds() + jitter)
}