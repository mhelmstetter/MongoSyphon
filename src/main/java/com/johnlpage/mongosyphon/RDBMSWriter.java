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
    private String user;
    private String pass;
    private Connection connection = null; 
    private Logger logger;
    ResultSet results = null;
    //PreparedStatement stmt = null;
    ResultSetMetaData metaData = null;
    int columnCount = 0;
    
    private Object parentId = null;

    private PreparedStatement insertStatement;
    private Map<String, PreparedStatement> childStatementsMap = new HashMap<String, PreparedStatement>();
    
    String stmttext=null;
    Document prevRow=null;
    
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
            childStatementsMap.put(key, childStatement);
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
        this.user = user;
        this.pass = pass;
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
            
            logger.info("Create");
            int count = 1;
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                populateStatement(insertStatement, key, value, count);
                count++;
            }
            insertStatement.executeUpdate();
            
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }
    
    private static boolean setScalarValueOnStatement(PreparedStatement stmt, int statementIndex, Object value) throws SQLException {
        boolean set = true;
        if (value.getClass() == String.class) {
            stmt.setString(statementIndex, (String)value);
        } else if (value.getClass() == Date.class) {
            Date d = (Date)value;
            //TODO check that this does correct TZ conversion?
            stmt.setDate(statementIndex, new java.sql.Date(d.getTime()));
        } else if (value.getClass() == ObjectId.class) {
            ObjectId oid = (ObjectId)value;
            stmt.setString(statementIndex, oid.toHexString());
        } else if (value.getClass() == Integer.class) {
            stmt.setInt(statementIndex, (Integer)value);
        } else {
            set = false;
        }
        return set;
    }
    
    private void populateStatement(PreparedStatement stmt, String key, Object value, int statementIndex) throws SQLException {
        
        // TODO use a stack? to handle deeper nesting
        if (stmt == insertStatement) {
            if (key.equals("_id")) {
                System.out.println(value);
                parentId = value;
            }
        }
        
        boolean set = setScalarValueOnStatement(stmt, statementIndex, value);
        if (! set) {
            if (value.getClass() == ArrayList.class) {
                int childIdx = 1;
                ArrayList list = (ArrayList)value;
                for (Object listElement : list) {
                    if (listElement.getClass() != Document.class) {
                        PreparedStatement childStatement = childStatementsMap.get(key);
                        setScalarValueOnStatement(childStatement, 1, parentId);
                        childStatement.setInt(2, childIdx);
                        setScalarValueOnStatement(childStatement, 3, listElement);
                        childStatement.executeUpdate();
                        
                    } else {
                        logger.warn("TODO nested Document!");
                    }
                    
                    System.out.println("** " + listElement);
                    childIdx++;
                }
            } else {
                logger.error("No type mapping for " + value.getClass().getName());
            }
        } else {
            // statement is executed by the caller
        }
        
          
    }

    public void close() {
        // TODO Auto-generated method stub
        
    }

}
