package de.cimt.talendcomp.exasol;

import org.junit.runners.Suite;
import org.junit.runner.RunWith;

@RunWith(Suite.class)
@Suite.SuiteClasses({ ExecSCD2JUnit.class, ExecSCD2JUnit2.class, ExecSCD2WithTimestampJUnit.class,
		ExecSCD3JUnit.class, ExecSCD3withoutSCD2JUnit.class })
public class ExecTestSuite {

}
