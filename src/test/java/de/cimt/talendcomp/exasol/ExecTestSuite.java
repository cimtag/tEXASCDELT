package de.cimt.talendcomp.exasol;

import org.junit.runners.Suite;
import org.junit.runner.RunWith;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	ExecSCD2ErrorTestJUnit.class, 
	ExecSCD2JUnit.class, 
	ExecSCD2JUnit2.class,
	ExecSCD2WithNullJUnit.class, 
	ExecSCD2WithTimestampJUnit.class, 
	ExecSCD3JUnit.class,
	ExecSCD3withoutSCD2JUnit.class, 
	Scd2JUnit.class, 
	ExecSCD1DuplicateKeyJUnit.class,
	ExecSCD1a2JUnit.class,
	ExecSCD1b2JUnit.class,

	Scd1JUnit.class
})
public class ExecTestSuite {

}
