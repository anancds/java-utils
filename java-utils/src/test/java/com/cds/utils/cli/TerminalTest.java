package com.cds.utils.cli;

import static org.junit.Assert.*;

import org.elasticsearch.cli.*;
import org.elasticsearch.test.ESTestCase;

/**
 * Created by chendongsheng5 on 2017/4/11.
 */
public class TerminalTest extends ESTestCase {
  public void testVerbosity() throws Exception {
    MockTerminal terminal = new MockTerminal();
    terminal.setVerbosity(org.elasticsearch.cli.Terminal.Verbosity.SILENT);
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.SILENT, "text");
    assertNotPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.NORMAL, "text");
    assertNotPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.VERBOSE, "text");

    terminal = new MockTerminal();
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.SILENT, "text");
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.NORMAL, "text");
    assertNotPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.VERBOSE, "text");

    terminal = new MockTerminal();
    terminal.setVerbosity(org.elasticsearch.cli.Terminal.Verbosity.VERBOSE);
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.SILENT, "text");
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.NORMAL, "text");
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.VERBOSE, "text");
  }

  public void testEscaping() throws Exception {
    MockTerminal terminal = new MockTerminal();
    assertPrinted(terminal, org.elasticsearch.cli.Terminal.Verbosity.NORMAL, "This message contains percent like %20n");
  }

  public void testPromptYesNoDefault() throws Exception {
    MockTerminal terminal = new MockTerminal();
    terminal.addTextInput("");
    assertTrue(terminal.promptYesNo("Answer?", true));
    terminal.addTextInput("");
    assertFalse(terminal.promptYesNo("Answer?", false));
    terminal.addTextInput(null);
    assertFalse(terminal.promptYesNo("Answer?", false));
  }

  public void testPromptYesNoReprompt() throws Exception {
    MockTerminal terminal = new MockTerminal();
    terminal.addTextInput("blah");
    terminal.addTextInput("y");
    assertTrue(terminal.promptYesNo("Answer? [Y/n]\nDid not understand answer 'blah'\nAnswer? [Y/n]", true));
  }

  public void testPromptYesNoCase() throws Exception {
    MockTerminal terminal = new MockTerminal();
    terminal.addTextInput("Y");
    assertTrue(terminal.promptYesNo("Answer?", false));
    terminal.addTextInput("y");
    assertTrue(terminal.promptYesNo("Answer?", false));
    terminal.addTextInput("N");
    assertFalse(terminal.promptYesNo("Answer?", true));
    terminal.addTextInput("n");
    assertFalse(terminal.promptYesNo("Answer?", true));
  }

  private void assertPrinted(MockTerminal logTerminal, org.elasticsearch.cli.Terminal.Verbosity verbosity, String text) throws Exception {
    logTerminal.println(verbosity, text);
    String output = logTerminal.getOutput();
    assertTrue(output, output.contains(text));
    logTerminal.reset();
  }

  private void assertNotPrinted(MockTerminal logTerminal, org.elasticsearch.cli.Terminal.Verbosity verbosity, String text) throws Exception {
    logTerminal.println(verbosity, text);
    String output = logTerminal.getOutput();
    assertTrue(output, output.isEmpty());
  }
}