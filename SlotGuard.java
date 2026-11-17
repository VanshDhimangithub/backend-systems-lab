import java.util.*;
import java.util.concurrent.*;

/** In-memory booking core. No network server or persistence is implied. */
public final class SlotGuard {
    public record Booking(UUID id, String slot, String customer) {}
    private record Request(String slot, String customer, Booking result) {}
    private final Map<String, Booking> slots = new HashMap<>();
    private final Map<String, Request> requests = new HashMap<>();

    // One lock deliberately makes the reservation + idempotency record atomic.
    public synchronized Booking book(String key, String slot, String customer) {
        for (String value : List.of(key, slot, customer)) {
            if (value.isBlank() || value.length() > 128) throw new IllegalArgumentException("Invalid identifier");
        }
        Request prior = requests.get(key);
        if (prior != null) {
            if (!prior.slot.equals(slot) || !prior.customer.equals(customer))
                throw new IllegalArgumentException("Idempotency key reused for a different request");
            return prior.result;
        }
        if (slots.containsKey(slot)) throw new IllegalStateException("Slot already booked");
        Booking booking = new Booking(UUID.randomUUID(), slot, customer);
        slots.put(slot, booking);
        requests.put(key, new Request(slot, customer, booking));
        return booking;
    }

    public synchronized int bookingCount() { return slots.size(); }

    public static void main(String[] args) throws Exception {
        SlotGuard service = new SlotGuard();
        Booking first = service.book("request-1", "doctor-1:2030-01-01T09:00Z", "patient-1");
        if (!first.equals(service.book("request-1", first.slot, first.customer))) throw new AssertionError("Retry changed booking");
        try { service.book("request-1", "different-slot", "patient-1"); throw new AssertionError("Payload mismatch accepted"); }
        catch (IllegalArgumentException expected) {}
        try { service.book("", "slot", "patient"); throw new AssertionError("Blank identifier accepted"); }
        catch (IllegalArgumentException expected) {}
        SlotGuard race = new SlotGuard();
        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i=0;i<100;i++) {
            final int id=i;
            results.add(pool.submit(() -> {
                start.await();
                try { race.book("request-"+id, "same-slot", "patient-"+id); return true; }
                catch (IllegalStateException occupied) { return false; }
            }));
        }
        start.countDown();
        int winners=0;
        try { for (Future<Boolean> result:results) if (result.get(10, TimeUnit.SECONDS)) winners++; }
        finally { pool.shutdownNow(); }
        if (winners != 1 || race.bookingCount() != 1) throw new AssertionError("Double booking: "+winners);
        System.out.println("PASS: retry identity, payload mismatch, validation, 100-way race (one winner)");
    }
}
