package tobepro.nebula.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class NebulaInvocationHandler implements InvocationHandler {
    public static final Logger logger = LoggerFactory.getLogger(NebulaInvocationHandler.class);
    private final Map<String, Method> methods = new HashMap<>();
    
    private final Object target;

    public NebulaInvocationHandler(Object target) {
        this.target = target;
        for (Method method : target.getClass().getMethods()) {
            String key = getUniqueKeyFromMethod(method);
            this.methods.put(key, method);
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            Object result = this.methods.get(getUniqueKeyFromMethod(method)).invoke(target, args);
            if (logger.isTraceEnabled()) {
                logger.trace("[" + target.getClass().getCanonicalName() + "] " + method.getName());
            }
            return result;
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private String getUniqueKeyFromMethod(Method method) {
        String key = method.getName() + "_";
        for(Class type : method.getParameterTypes()) {
            key += type.getCanonicalName() + "_";
        }
        return key;
    }
}
