package ch.usi.dslab.lel.dynastar.tpcc.benchmark;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.sense.monitors.LatencyDistributionPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastar.tpcc.TpccCommandType;
import ch.usi.dslab.lel.dynastar.tpcc.TpccTerminal;
import ch.usi.dslab.lel.dynastar.tpcc.TpccTerminalV2;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public interface BenchContext {
    class CallbackContext {
        private static AtomicLong lastReqId = new AtomicLong();
        long startTimeNano;
        long timelineBegin;
        TpccCommandType commandType;
        long reqId;
        CompletableFuture<Message> callback = null;
        TpccTerminal terminal;


        public CallbackContext(TpccTerminal terminal, TpccCommandType commandType) {
            this.terminal = terminal;
            this.startTimeNano = System.nanoTime();
            this.timelineBegin = System.currentTimeMillis();
            this.commandType = commandType;
            this.reqId = lastReqId.incrementAndGet();
        }

        public CallbackContext(TpccCommandType commandType, CompletableFuture callback) {
            this.startTimeNano = System.nanoTime();
            this.timelineBegin = System.currentTimeMillis();
            this.commandType = commandType;
            this.reqId = lastReqId.incrementAndGet();
            this.callback = callback;
        }

        public CallbackContext(long startTimeNano, long timelineBegin, TpccCommandType commandType) {
            this.startTimeNano = startTimeNano;
            this.timelineBegin = timelineBegin;
            this.commandType = commandType;
            this.reqId = lastReqId.incrementAndGet();
        }
    }

    class CallbackHandler implements BiConsumer {
        private LatencyPassiveMonitor overallLatencyMonitor, newOrderLatencyMonitor, paymentLatencyMonitor, deliveryLatencyMonitor, orderStatusLatencyMonitor, stockLevelLatencyMonitor;
        private ThroughputPassiveMonitor overallThroughputMonitor, newOrderThroughputMonitor, paymentThroughputMonitor, deliveryThroughputMonitor, orderStatusThroughputMonitor, stockLevelThroughputMonitor;
        //        private TimelinePassiveMonitor overallTimelineMonitor, postTimelineMonitor, followTimelineMonitor, unfollowTimelineMonitor, getTimelineTimelineMonitor;
        private LatencyDistributionPassiveMonitor cdfMonitor;
//        private TpccTerminal parentRunner;

        public CallbackHandler(int clientId) {
            String client_mode = "client";
            // Latency monitors
            overallLatencyMonitor = new LatencyPassiveMonitor(clientId, client_mode + "_overall");
            newOrderLatencyMonitor = new LatencyPassiveMonitor(clientId, client_mode + "_new_order", false);
            paymentLatencyMonitor = new LatencyPassiveMonitor(clientId, client_mode + "_payment", false);
            deliveryLatencyMonitor = new LatencyPassiveMonitor(clientId, client_mode + "_delivery", false);
            orderStatusLatencyMonitor = new LatencyPassiveMonitor(clientId, client_mode + "_order_status", false);
            stockLevelLatencyMonitor = new LatencyPassiveMonitor(clientId, client_mode + "_stock_level", false);
            cdfMonitor = new LatencyDistributionPassiveMonitor(clientId, client_mode + "_overall");
            LatencyDistributionPassiveMonitor.setBucketWidthNano(100);

            // Throughput monitors
            overallThroughputMonitor = new ThroughputPassiveMonitor(clientId, client_mode + "_overall");
            newOrderThroughputMonitor = new ThroughputPassiveMonitor(clientId, client_mode + "_new_order", false);
            paymentThroughputMonitor = new ThroughputPassiveMonitor(clientId, client_mode + "_payment", false);
            deliveryThroughputMonitor = new ThroughputPassiveMonitor(clientId, client_mode + "_delivery", false);
            orderStatusThroughputMonitor = new ThroughputPassiveMonitor(clientId, client_mode + "_order_status", false);
            stockLevelThroughputMonitor = new ThroughputPassiveMonitor(clientId, client_mode + "_stock_level", false);

            // Timeline monitor
//            overallTimelineMonitor = new TimelinePassiveMonitor(clientId, client_mode + "_overall");
//            postTimelineMonitor = new TimelinePassiveMonitor(clientId, client_mode + "_post", false);
//            followTimelineMonitor = new TimelinePassiveMonitor(clientId, client_mode + "_follow", false);
//            unfollowTimelineMonitor = new TimelinePassiveMonitor(clientId, client_mode + "_unfollow", false);
//            getTimelineTimelineMonitor = new TimelinePassiveMonitor(clientId, client_mode + "_gettimeline", false);

        }


        @Override
        public void accept(Object reply, Object context) {

//            Message replyMsg = (Message) reply;
//            ((Message) reply).rewind();
            CallbackContext benchContext = (CallbackContext) context;
            long nowNano = System.nanoTime();
            long nowClock = System.currentTimeMillis();
            ((CallbackContext) context).terminal.releasePermit();

            // log latency
            overallLatencyMonitor.logLatency(benchContext.startTimeNano, nowNano);
//            overallTimelineMonitor.logTimeline("client_send", benchContext.timelineBegin,
//                    "oracle_deliver", replyMsg.t_oracle_deliver,
//                    "oracle_dequeued", replyMsg.t_oracle_dequeued,
//                    "oracle_execute", replyMsg.t_oracle_execute,
//                    "oracle_execute_finish", replyMsg.t_oracle_finish_excecute,
//                    "partition_devlier", replyMsg.t_partition_deliver,
//                    "partition_dequeued", replyMsg.t_partition_dequeued,
//                    "partition_execute", replyMsg.t_partition_execute,
//                    "partition_execute_finish", replyMsg.t_partition_finish_excecute,
//                    "client_received", nowClock,
//                    "object_count", benchContext.nodeSize);
            cdfMonitor.logLatencyForDistribution(benchContext.startTimeNano, nowNano);

            // increment throughput count
            overallThroughputMonitor.incrementCount();

            LatencyPassiveMonitor requestLatencyMonitor = null;
//            TimelinePassiveMonitor requestTimelineMonitor = null;
            ThroughputPassiveMonitor requestThroughputMonitor = null;

//            if (((Message) reply).peekNext().equals("OK") && benchContext.callback != null) {
//                try {
//                    benchContext.callback.complete((Message) reply);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
            switch (benchContext.commandType) {
                case NEW_ORDER:
                    requestLatencyMonitor = newOrderLatencyMonitor;
//                    requestTimelineMonitor = postTimelineMonitor;
                    requestThroughputMonitor = newOrderThroughputMonitor;
                    break;
                case PAYMENT:
                    requestLatencyMonitor = paymentLatencyMonitor;
//                    requestTimelineMonitor = followTimelineMonitor;
                    requestThroughputMonitor = paymentThroughputMonitor;
                    break;
                case DELIVERY:
                    requestLatencyMonitor = deliveryLatencyMonitor;
//                    requestTimelineMonitor = unfollowTimelineMonitor;
                    requestThroughputMonitor = deliveryThroughputMonitor;
                    break;
                case ORDER_STATUS:
                    requestLatencyMonitor = orderStatusLatencyMonitor;
//                    requestTimelineMonitor = getTimelineTimelineMonitor;
                    requestThroughputMonitor = orderStatusThroughputMonitor;
                    break;
                case STOCK_LEVEL:
                    requestLatencyMonitor = stockLevelLatencyMonitor;
//                    requestTimelineMonitor = getTimelineTimelineMonitor;
                    requestThroughputMonitor = stockLevelThroughputMonitor;
                    break;
                default:
                    break;
            }
//            if (!((Message) reply).peekNext().equals("DROPPED"))
//                requestLatencyMonitor.logLatency(benchContext.startTimeNano, nowNano);
////            if (nowNano - benchContext.startTimeNano > 200000000)
////                System.out.println("slow: " + reply + " - " + (nowNano - benchContext.startTimeNano) + " size " + benchContext.nodeSize);
////            requestTimelineMonitor.logTimeline("client_send", benchContext.timelineBegin,
////                    "oracle_deliver", replyMsg.t_oracle_deliver,
////                    "oracle_dequeued", replyMsg.t_oracle_dequeued,
////                    "oracle_execute", replyMsg.t_oracle_execute,
////                    "oracle_execute_finish", replyMsg.t_oracle_finish_excecute,
////                    "partition_devlier", replyMsg.t_partition_deliver,
////                    "partition_dequeued", replyMsg.t_partition_dequeued,
////                    "partition_execute", replyMsg.t_partition_execute,
////                    "partition_execute_finish", replyMsg.t_partition_finish_excecute,
////                    "client_received", nowClock,
////                    "object_count", benchContext.nodeSize);
//            if (!((Message) reply).peekNext().equals("DROPPED")) requestThroughputMonitor.incrementCount();
        }

    }
}
