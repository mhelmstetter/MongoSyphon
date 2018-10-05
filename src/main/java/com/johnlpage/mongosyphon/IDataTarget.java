package com.johnlpage.mongosyphon;

import org.bson.Document;

public interface IDataTarget {

    Document FindOne(Document query, Document fields, Document order);

    //Document shoudl have a 'find' field
    void Update(Document doc, boolean upsert);

    void Save(Document doc);

    void Create(Document doc);

    void close();

}