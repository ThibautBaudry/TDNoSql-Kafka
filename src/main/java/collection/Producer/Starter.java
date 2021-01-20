package collection.Producer;

public class Starter {

    public static void main(String[] args) throws InterruptedException {
        KafkaPublisher publisher = new KafkaPublisher();
        Collector collector = new Collector(publisher);
        scheduleCollect(collector);
    }

    private static void scheduleCollect(Collector collector) throws InterruptedException {
        ScheduledExecutorRepeat executorRepeat = new ScheduledExecutorRepeat(collector, 10);
        executorRepeat.repeat();
    }
}
