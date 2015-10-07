package org.rakam.kume;


public interface MigrationListener {
    void migrationStart(Member sourceMember);
    void migrationEnd(Member sourceMember);
}
