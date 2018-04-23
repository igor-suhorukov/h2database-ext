package com.github.igorsuhorukov.h2;

import org.basex.core.Context;
import org.basex.query.QueryException;
import org.basex.query.QueryProcessor;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.h2.tools.Csv;

import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Map;

public class XQuery3 {

    public static ResultSet query(String xquery) throws QueryException, IOException {
        return query(xquery, null);
    }

    public static ResultSet query(String xquery, Map<String, Object> parameters) throws QueryException, IOException {
        try (QueryProcessor queryProcessor = new QueryProcessor(xquery, new Context())) {
            bindParameters(queryProcessor, parameters);
            Value queryResult = queryProcessor.value();
            Iterator<Item> resultIterator = queryResult.iterator();
            if(!resultIterator.hasNext()){
                throw new IllegalArgumentException("Query result must be csv values with headers");
            }
            Item xqResultSet = resultIterator.next();
            String csvResult = Item.toString(xqResultSet.string(null), false, false);
            if(csvResult!=null && !csvResult.isEmpty()) {
                return new Csv().read(new StringReader(csvResult.replace("&#xA;","\n")), null);
            } else {
                return null;
            }
        }
    }

    private static void bindParameters(QueryProcessor queryProcessor, Map<String, Object> parameters) {
        if(parameters!=null && !parameters.isEmpty()) {
            parameters.forEach((name, parameter) -> {
                try {
                    queryProcessor.bind(name, parameter);
                } catch (QueryException e) {
                    throw new IllegalArgumentException(name, e);
                }
            });
        }
    }
}
