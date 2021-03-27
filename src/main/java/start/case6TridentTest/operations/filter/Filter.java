package start.case6TridentTest.operations.filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class Filter {
    public static class BiggerFilter extends BaseFilter {
        int filter;

        public BiggerFilter(int filter) {
            this.filter = filter;
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            int _amt = tuple.getIntegerByField("amt");
            return _amt >= filter;
        }
    }

    public static class SmallerFilter extends BaseFilter {
        int filter;

        public SmallerFilter(int filter) {
            this.filter = filter;
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            int _amt = tuple.getIntegerByField("amt");
            return _amt <= filter;
        }
    }

    public static class StringFilter extends BaseFilter {
        String[] filter;

        public StringFilter(String[] filter) {
            this.filter = filter;
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            String word = tuple.getStringByField("word");
            for (String s : filter) {
                if (s.contains(word)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class PrintFilter extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            System.out.println("result >>> " + tuple);
            return false;

        }
    }
}
