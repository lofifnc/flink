package api.input;

import input.Input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InputBuilder<T> implements Input<T> {
	List<T> input = new ArrayList<T>();

	public void add(T elem){
		input.add(elem);
	}

	public void addAll(Collection<T> elems) {
		input.addAll(elems);
	}

	@Override
	public List<T> getInput() {
		return null;
	}
}
