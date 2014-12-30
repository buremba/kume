package org.rakam.kume.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 01/12/14 03:03.
 */
public class Throwables {
    final static Logger LOGGER = LoggerFactory.getLogger(Throwables.class);

    public static Runnable propagate(Runnable callable){
        try {
            callable.run();
        } catch (Exception e) {
            LOGGER.error("error while running throwable code block", e);
        }
        return callable;
    }
}
