import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Small immutable class for containing a timestamp
 */
@SuppressWarnings("ALL")
public class Timestamp implements Serializable, Comparable<Timestamp> {
  final int epoch;
  final int counter;

  public Timestamp(int epoch, int counter) {
    this.epoch = epoch;
    this.counter = counter;
  }

  @Override
  public int compareTo(@NotNull Timestamp o) {
    if (epoch == o.epoch) {
      return Integer.compare(counter, o.counter);
    }
    return Integer.compare(epoch, o.epoch);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Timestamp timeStamp = (Timestamp) o;
    return epoch == timeStamp.epoch && counter == timeStamp.counter;
  }

  @Override
  public int hashCode() {
    return 31 * epoch + counter;
  }

  public Timestamp nextCounterTimestamp() {
    return new Timestamp(epoch, counter);
  }
}
