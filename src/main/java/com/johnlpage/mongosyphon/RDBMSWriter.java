package com.johnlpage.mongosyphon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDBMSWriter implements IDataTarget {
    
    private String connectionString = null;
    private Connection connection = null; 
    private Logger logger;
    ResultSet results = null;
    //PreparedStatement stmt = null;
    ResultSetMetaData metaData = null;
    int columnCount = 0;
    
    private Object parentId = null;

    private PreparedStatement insertStatement;
    private Map<String, StatementWrapper> childStatementsMap = new HashMap<String, StatementWrapper>();
    
    String stmttext=null;
    Document prevRow=null;
    
    private int opsCount = 0;
    
    
    public RDBMSWriter(String connectionString, String user, String pass, Document insertConfig) throws SQLException {
        logger = LoggerFactory.getLogger(RDBMSWriter.class);
        this.connectionString = connectionString;
        Connect(user, pass);
        String rootInsertSql = insertConfig.getString("root");
        insertStatement = connection.prepareStatement(rootInsertSql);
        for (Map.Entry<String, Object> entry : insertConfig.entrySet()) {
            String key = entry.getKey();
            if (key.equals("root")) {
                continue;
            }
            String childSql = (String)entry.getValue();
            PreparedStatement childStatement = connection.prepareStatement(childSql);
            childStatementsMap.put(key, new StatementWrapper(childStatement));
        }
    }
    
    private void Connect(String user, String pass) {
        try {
            connection = DriverManager.getConnection(connectionString, user,
                    pass);
        } catch (SQLException e) {
            logger.error("Unable to connect to RDBMS");
            logger.error(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Document FindOne(Document query, Document fields, Document order) {
        // TODO Auto-generated method stub
        return null;
    }

    public void Update(Document doc, boolean upsert) {
        // TODO Auto-generated method stub
        
    }

    public void Save(Document doc) {
        logger.info("Save");
        
    }

    public void Create(Document doc) {
        try {
            
            //logger.info("Create");
            int count = 1;
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                boolean set = populateStatement(insertStatement, key, value, count);
                if (set) {
                    count++;
                }
            }
            //insertStatement.executeUpdate();
            insertStatement.addBatch();
            
            FlushOpsIfFull();
            
            
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }
    
    private static boolean setScalarValueOnStatement(PreparedStatement stmt, int statementIndex, Object value) throws SQLException {
        boolean set = true;
        if (value.getClass() == String.class) {
            String strVal =  (String)value;
            // TODO parameterize this hack
            if (strVal.length() > 1024) {
                strVal = strVal.substring(0, 1023);
            }
            stmt.setString(statementIndex, strVal);
        } else if (value.getClass() == Date.class) {
            Date d = (Date)value;
            //TODO check that this does correct TZ conversion?
            stmt.setDate(statementIndex, new java.sql.Date(d.getTime()));
        } else if (value.getClass() == ObjectId.class) {
            ObjectId oid = (ObjectId)value;
            stmt.setString(statementIndex, oid.toHexString());
        } else if (value.getClass() == Integer.class) {
            stmt.setInt(statementIndex, (Integer)value);
        } else if (value.getClass() == Boolean.class) {
            stmt.setBoolean(statementIndex, (Boolean)value);
        } else {
            set = false;
        }
        return set;
    }
    
    private boolean populateStatement(PreparedStatement stmt, String key, Object value, int statementIndex) throws SQLException {
        
        // TODO use a stack? to handle deeper nesting
        if (stmt == insertStatement) {
            if (key.equals("_id")) {
                parentId = value;
            }
        }
        //System.out.println(statementIndex + " " + value);
        boolean set = setScalarValueOnStatement(stmt, statementIndex, value);
        
        if (! set) {
            if (value.getClass() == ArrayList.class) {
                int childIdx = 1;
                ArrayList list = (ArrayList)value;
                for (Object listElement : list) {
                    if (listElement.getClass() != Document.class) {
                        StatementWrapper wrapper = childStatementsMap.get(key); 
                        PreparedStatement childStatement = wrapper.statement;
                        setScalarValueOnStatement(childStatement, 1, parentId);
                        childStatement.setInt(2, childIdx);
                        setScalarValueOnStatement(childStatement, 3, listElement);
                        //childStatement.executeUpdate();
                        childStatement.addBatch();
                        wrapper.statementCount++;
                        if (wrapper.statementCount % 1000 == 0) {
                            childStatement.executeBatch();
                        }
                        
                    } else {
                        logger.warn("TODO nested Document!");
                    }
                    childIdx++;
                }
                // skip set for arrays, since they are populated into a child table
            } else {
                logger.error("No type mapping for " + value.getClass().getName());
            }
        } else {
            // statement is executed by the caller
        }
        return set;
          
    }

    public void close() {
        try {
            insertStatement.executeBatch();
            for (StatementWrapper wrapper : childStatementsMap.values()) {
                wrapper.statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void FlushOpsIfFull() {
        opsCount++;
        
        if (opsCount % 1000 == 0) {
            try {
                insertStatement.executeBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    class StatementWrapper {
        
        public StatementWrapper(PreparedStatement statement) {
            this.statement = statement;
            this.statementCount = 0;
        }
        
        public PreparedStatement statement;
        public int statementCount;
    }

}
