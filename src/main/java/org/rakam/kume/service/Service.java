package org.rakam.kume.service;


import org.rakam.kume.Operation;
import org.rakam.kume.Request;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:04.
 */
public interface Service {
    void handleOperation(Operation operation);
    Object handleRequest(Request request);

    void onStart();
    void onClose();
}