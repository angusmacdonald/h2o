package org.h2o.event.gui;
import java.awt.BorderLayout;
import java.util.LinkedList;
import java.util.Queue;

import javax.swing.JTextPane;

import javax.swing.JFrame;
import javax.swing.UIManager;
import javax.swing.WindowConstants;

import org.h2o.event.client.H2OEvent;
import org.h2o.event.server.EventServer;
import org.h2o.event.server.handlers.EventHandler;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class BasicEventGui extends JFrame implements EventHandler {

	private static final long serialVersionUID = 1513563224158004155L;
	private JTextPane eventOutput;

	private Queue<H2OEvent> events = new LinkedList<H2OEvent>();

	/**
	 * Auto-generated main method to display this JFrame
	 */
	public static void main(String[] args) {

		BasicEventGui gui = new BasicEventGui();
		gui.setLocationRelativeTo(null); //centre
		gui.setVisible(true);
	}

	public BasicEventGui() {
		initGUI();

		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		EventServer server = new EventServer(EventServer.EVENT_SERVER_PORT, this);
		server.start();
	}

	private void initGUI() {
		try {
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

			eventOutput = new JTextPane();
			getContentPane().add(eventOutput, BorderLayout.CENTER);

			eventOutput.setText("H2O Database Event Monitor. Output:");

			pack();
			setSize(900, 500);
		} catch (Exception e) {}
	}

	@Override
	public boolean pushEvent(H2OEvent event) {

		events.add(event);

		eventOutput.setText(eventOutput.getText() + "\n" + event);
		return true;

	}

}
