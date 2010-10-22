package org.h2o.viewer.gui;

import java.awt.BorderLayout;
import java.util.LinkedList;
import java.util.Queue;

import javax.swing.JFrame;
import javax.swing.JTextPane;
import javax.swing.UIManager;
import javax.swing.WindowConstants;

import org.h2o.viewer.gwt.client.H2OEvent;
import org.h2o.viewer.server.EventServer;
import org.h2o.viewer.server.handlers.EventHandler;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class BasicEventGui extends JFrame implements EventHandler {

    private static final long serialVersionUID = 1513563224158004155L;

    private JTextPane eventOutput;

    private final Queue<H2OEvent> events = new LinkedList<H2OEvent>();

    /**
     * Auto-generated main method to display this JFrame
     */
    public static void main(final String[] args) {

        final BasicEventGui gui = new BasicEventGui();
        gui.setLocationRelativeTo(null); // centre
        gui.setVisible(true);
    }

    public BasicEventGui() {

        initGUI();

        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
        Diagnostic.setLevel(DiagnosticLevel.INIT);
        final EventServer server = new EventServer(EventServer.EVENT_SERVER_PORT, this);
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
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean pushEvent(final H2OEvent event) {

        events.add(event);

        eventOutput.setText(eventOutput.getText() + "\n" + event);
        return true;

    }

}