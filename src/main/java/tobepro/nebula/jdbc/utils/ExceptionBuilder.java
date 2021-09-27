package tobepro.nebula.jdbc.utils;

public class ExceptionBuilder {
    private ExceptionBuilder() {}
    
    public static UnsupportedOperationException buildUnsupportedOperationException() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        if (stackTraceElements.length > 2) {
            StackTraceElement caller = stackTraceElements[2];

            String sb = "Method " + caller.getMethodName() + " in class " + caller.getClassName() +
                    " is not yet implemented.";
            return new UnsupportedOperationException(sb);
        } else {
            return new UnsupportedOperationException("Not yet implemented.");
        }
    }
}
