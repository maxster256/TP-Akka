package Pi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class Pi {

    public static void main (String[] args) {
        Pi pi = new Pi();

        int numberOfWorkers = 2;
        int numberOfElements = 1000;
        int numberOfMessages = 100000;
        // total number of summed elements = nrOfElements x nrOfMessages

        pi.calculate(numberOfWorkers, numberOfElements, numberOfMessages);
    }

    static class Calculate {

    }

    static class Work {
        private final int start;
        private final int numberOfElements;

        Work (int start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        public int getStart () {
            return start;
        }

        public int getNumberOfElements() {
            return numberOfElements;
        }
    }

    static class Result {
        private final double value;

        Result (double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }
    }

    static class PiApproximation {
        private final double pi;
        private final Duration duration;

        PiApproximation (double pi, Duration duration) {
            this.pi = pi;
            this.duration = duration;
        }

        public double getPi() {
            return pi;
        }

        public Duration getDuration() {
            return duration;
        }
    }

    public static class Worker extends AbstractActor {

        private double calculatePiFor (int start, int numberOfElements) {
            double acc = 0.0;

            for (int i = start * numberOfElements; i <= ((start + 1) * numberOfElements - 1); i++) {
                acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
            }
            return acc;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Work.class, work -> {
                        double result = calculatePiFor (work.getStart(), work.getNumberOfElements());
                        getSender().tell (new Result (result), getSelf());

                    })
                    .build();
        }
    }

    public static class Master extends AbstractActor {

        private final int numberOfMessages;
        private final int numberOfElements;

        private double pi;
        private int numberOfResults;
        private final long start = System.currentTimeMillis();

        private final ActorRef listener;
        private final ActorRef workerRouter;
        //private final Router router;

        Master (int numberOfWorkers, int numberOfMessages, int numberOfElements, ActorRef listener) {
            this.numberOfElements = numberOfElements;
            this.numberOfMessages = numberOfMessages;
            this.listener = listener;

            workerRouter = getContext().actorOf(new RoundRobinPool(numberOfWorkers).props(Props.create(Worker.class)),
                    "workerRouter");

        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Calculate.class, calculate -> {
                        for (int start = 0; start < numberOfMessages; start++) {
                            workerRouter.tell(new Work(start, numberOfElements), getSelf());
                        }
                    })
                    .match(Result.class, result -> {
                        pi += result.getValue();
                        numberOfResults += 1;

                        if (numberOfResults == numberOfMessages) {
                            Duration duration = Duration.create(System.currentTimeMillis() - start,
                                    TimeUnit.MILLISECONDS);
                            listener.tell(new PiApproximation(pi, duration), getSelf());
                            getContext().stop(getSelf());
                        }
                    })
                    .build();
        }
    }

    public static class Listener extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(PiApproximation.class, approximation -> {
                        System.out.println(String.format("\n\tPi approximation: " +
                                        "\t\t%s\n\tCalculation time: \t%s",
                                approximation.getPi(), approximation.getDuration()));
                        getContext().system().terminate();
                    })
                    .build();
        }
    }

    public void calculate (final int numberOfWorkers, final int numberOfElements, final int numberOfMessages) {
        ActorSystem system = ActorSystem.create("PiSystem");

        final ActorRef listener = system.actorOf(Props.create(Listener.class), "listener");
        // start the calculation

        ActorRef master = system.actorOf(Props.create(Master.class, numberOfWorkers, numberOfMessages,
                numberOfElements, listener), "master");
        master.tell(new Calculate(), ActorRef.noSender());

    }
}
