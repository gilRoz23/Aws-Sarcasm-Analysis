    // Pair class to hold an integer and a string
    class Pair {
        int intValue;
        String stringValue;

        Pair(int intValue, String stringValue) {
            this.intValue = intValue;
            this.stringValue = stringValue;
        }
        int getIntValue() {
            return intValue;
        }

        void setIntValue(int intValue) {
            this.intValue = intValue;
        }
        String getStringValue() {
            return stringValue;
        }
        void setStringValue(String stringValue) {
            this.stringValue = stringValue;
        }

        @Override
        public String toString() {
            return "(" + intValue + ", " + stringValue + ")";
        }
    }