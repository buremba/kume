package org.rakam.kume.util;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 21:52.
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class CodeAnalyzerProcessor extends AbstractProcessor {

    @Override
    public boolean process(Set<? extends TypeElement> typeElements, RoundEnvironment roundEnvironment) {
        File file = new File("/tmp/test");
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("not doing much right now");
        return true;
    }
}