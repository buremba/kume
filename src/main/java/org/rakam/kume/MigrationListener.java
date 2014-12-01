package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/11/14 18:50.
 */
public interface MigrationListener {
    void migrationStart(Member sourceMember);
    void migrationEnd(Member sourceMember);
}
