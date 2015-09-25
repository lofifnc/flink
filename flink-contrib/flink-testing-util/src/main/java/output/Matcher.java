package output;

import java.util.Collection;

public interface Matcher<T> {
	boolean check(Collection<T> output);
}
