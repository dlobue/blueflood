package com.rackspacecloud.blueflood.inputs.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.Metric;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class DiscoveryWriter extends AsyncFunctionWithThreadPool<List<List<Metric>>, List<List<Metric>>> {

    private final List<DiscoveryIO> discoveryIOs = new ArrayList<DiscoveryIO>();

    public DiscoveryWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
    }

    public void registerIO(DiscoveryIO io) {
        discoveryIOs.add(io);
    }

    @Override
    public ListenableFuture<List<List<Metric>>> apply(List<List<Metric>> input) throws Exception {

        final List<ListenableFuture<Boolean>> resultFutures = new ArrayList<ListenableFuture<Boolean>>();

        for (List<Metric> metrics : input) {
            final List<Metric> batch = metrics;
            ListenableFuture<Boolean> futureBatchResult = getThreadPool().submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    try {
                        for (DiscoveryIO io : discoveryIOs) {
                            io.insertDiscovery(batch);
                        }
                        return true;
                    } catch (Exception ex) {
                        getLogger().error(ex.getMessage(), ex);
                        return false;
                    }
                }
            });

            resultFutures.add(futureBatchResult);
        }

        return new NoOpFuture<List<List<Metric>>>(input);
    }
}