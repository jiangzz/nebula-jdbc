package tobepro.nebula.jdbc.utils;

import tobepro.nebula.jdbc.ListArray;
import tobepro.nebula.jdbc.NebulaAbstractStatement;
import tobepro.nebula.jdbc.NebulaConnection;
import tobepro.nebula.jdbc.types.NebulaEdge;
import tobepro.nebula.jdbc.types.NebulaNode;
import tobepro.nebula.jdbc.types.NebulaTag;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.*;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.sql.Date;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Function;

public class NebulaUtil {
    private static final Logger logger = LoggerFactory.getLogger(NebulaUtil.class);
    public static final List<Class<?>> SUPPORT_CONVERT_TYPES = Arrays.asList(
            Boolean.class,
            Short.class,
            Integer.class,
            Long.class,
            String.class,
            Time.class,
            Date.class,
            Timestamp.class,
            NebulaNode.class,
            NebulaEdge.class,
            ListArray.class,
            Object.class
    );
    public static <T> T execute(NebulaAbstractStatement statement, String sql, Function<ResultSet, T> function) throws SQLException {
        try {
            Session session = ((NebulaConnection)statement.getConnection()).getSession();
            if (logger.isDebugEnabled()) {
                logger.debug("Execute SQL: " + sql);
            }
            ResultSet result = session.execute(sql);
            if (!result.isSucceeded()) {
                throw new SQLException(MessageFormat.format("\nNebula Error: {0} \n {1}", result.getErrorCode(), result.getErrorMessage()));
            }
            return function.apply(result);
        } catch (IOErrorException e) {
            throw new SQLException(e);
        }
    }

    public static int calculateUpdateCount(ResultSet result) {
        // 目前Nebula一次只能更新一条
        return 1;
    }

    /**
     * 根据valueWrapper类型字段转换，如果有多种适配类型（类似short,int,long）,则按照指定类型T进行转换
     * @param valueWrapper nebula元数据
     * @param type 待转换类型, 目前支持以下类型: Boolean,Short,Integer,Long,String,Time,Date,Timestamp,NebulaNode,NebulaEdge,ListArray or null
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertToJavaType(ValueWrapper valueWrapper, Class<T> type) throws SQLException {
        if (Objects.nonNull(type) && !SUPPORT_CONVERT_TYPES.contains(type)) {
            throw new SQLException("convert type '" + type + "' have not supported");
        }
        if (valueWrapper.isNull()) {
            return null;
        } else if (valueWrapper.isBoolean()) {
            return (T) Boolean.valueOf(valueWrapper.asBoolean());
        } else if (valueWrapper.isLong()) {
            if (type == Short.class) {
                return (T) Short.valueOf((short) valueWrapper.asLong());
            } else if (type == Integer.class) {
                return (T) Integer.valueOf((int) valueWrapper.asLong());
            } else {
                return (T) Long.valueOf(valueWrapper.asLong());
            }
        } else if(valueWrapper.isDouble()) {
            if (type == Float.class) {
                return (T) Float.valueOf((float) valueWrapper.asDouble());
            } else {
                return (T) Double.valueOf((float) valueWrapper.asDouble());
            }
        } else if(valueWrapper.isString()) {
            try {
                return (T) valueWrapper.asString();
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e);
            }
        } else if(valueWrapper.isTime()) {
            TimeWrapper timeWrapper = valueWrapper.asTime();
            return (T) Time.valueOf(LocalTime.of(timeWrapper.getHour(),
                                             timeWrapper.getMinute(),
                                             timeWrapper.getSecond(),
                                             timeWrapper.getMicrosec()));
        } else if(valueWrapper.isDate()) {
            DateWrapper dateWrapper = valueWrapper.asDate();
            return (T) Date.valueOf(LocalDate.of(dateWrapper.getYear(), dateWrapper.getMonth(), dateWrapper.getDay()));
        } else if(valueWrapper.isDateTime()) {
            DateTimeWrapper dateTimeWrapper = valueWrapper.asDateTime();
            return (T) Timestamp.valueOf(LocalDateTime.of(
                    dateTimeWrapper.getYear(),
                    dateTimeWrapper.getMonth(),
                    dateTimeWrapper.getDay(),
                    dateTimeWrapper.getHour(),
                    dateTimeWrapper.getMinute(),
                    dateTimeWrapper.getSecond(),
                    dateTimeWrapper.getMicrosec()
            ));
        } else if (valueWrapper.isVertex()) {
            try {
                Node node = valueWrapper.asNode();
                NebulaNode nebulaNode = new NebulaNode();
                nebulaNode.setVid(parseVid(node.getId()));
                nebulaNode.setTags(parseTags(node));
                return (T) nebulaNode;
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e);
            }
        } else if (valueWrapper.isEdge()) {
            Relationship rl = valueWrapper.asRelationship();
            NebulaEdge edge = new NebulaEdge();
            edge.setSrcVid(parseVid(rl.srcId()));
            edge.setDstVid(parseVid(rl.dstId()));
            edge.setName(rl.edgeName());
            edge.setRanking(rl.ranking());
            
            Map<String, Object> props = new HashMap<>();
            try {
                for (Map.Entry<String, ValueWrapper> entry : rl.properties().entrySet()) {
                    props.put(entry.getKey(), convertToJavaType(entry.getValue(), Object.class));
                }
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e);
            }
            edge.setProperties(props);
            return (T) edge;
        } else if (valueWrapper.isPath()) {
            // TODO 支持Path类型
            throw new SQLException("Path type not supported yet");
        } else if(valueWrapper.isList()) {
            // nebula map and set not supported yet
            return (T) new ListArray(valueWrapper.asList());
        } else {
            throw new SQLException("Object type not supported : " + type);
        }
    }

    private static List<NebulaTag> parseTags(Node node) throws SQLException {
        List<NebulaTag> tagList = new ArrayList<>();
        for (String tagName : node.tagNames()) {
            NebulaTag tag = new NebulaTag();
            tag.setName(tagName);
            try {
                Map<String, Object> nebulaProps = new HashMap<>();
                Map<String, ValueWrapper> properties = node.properties(tagName);
                for (Map.Entry<String, ValueWrapper> entry : properties.entrySet()) {
                    nebulaProps.put(entry.getKey(), convertToJavaType(entry.getValue(), Object.class));
                }
                tag.setProperties(nebulaProps);
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e);
            }
            tagList.add(tag);
        }
        return tagList;
    }

    private static Object parseVid(ValueWrapper valueWrapper) throws SQLException {
        if (valueWrapper.isString()) {
            try {
                return valueWrapper.asString();
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e);
            }
        } else if (valueWrapper.isLong()) {
            return valueWrapper.asLong();
        } else {
            throw new SQLException("vid type not supported");
        }
    }
}
