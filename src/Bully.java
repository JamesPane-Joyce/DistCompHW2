import java.util.LinkedList;

/**
 * Created by John on 11/15/2016.
 * This is meant to be an class capable of holding elections to determine who the leader should be at any
 * time that a failure is detected. Along with this, the objects shall also hold the information for who the
 * current leader actually is.
 */

public class Bully {

  private LinkedList<String> completeGraphOfNodes;
  private String leader;
  private boolean holdingElection;

  public Bully(LinkedList<String> allKnownNodes) {
    leader = "";
    holdingElection = false;
    completeGraphOfNodes = allKnownNodes;
  }

  public void updateNodeGraph(LinkedList<String> newGraph) {
    completeGraphOfNodes = newGraph;
  }

  public void initiateElection() {
    holdingElection = true;
    //ToDo Send ELECTION message to all process with higher idS
    //Note: The IDs should take the form of highest (epoch, counter) of log transactions where
    //epoch = an integer that is incremented every time a new leader is elected
    //counter = an integer that is incremented before every broadcast

    //ToDo wait for T time to receive an OK from those processes
    //Note: T = 2Tm + Tp (2 times the time for a message to travel + the time for a message to be processed)

    //ToDo If NOT received OK

    //ToDo leader = self

    //ToDo Send COORDINATOR message to all processes with lower ids

    holdingElection = false;

    //ToDo If received OK (can just be an else)

    //ToDo Wait T' time to receive COORDINATOR messages
    //Note: T' = 2Tm + 2Tp after the last okay was received

    //ToDo If we have received a COORDINATOR from process Q

    //ToDo leader = Q

    //ToDo Else

    initiateElection();

  }

  //This is called when we receive an ELECTION message from a process with a lower id
  public void onReceiveELECTION(String senderOfMessage) {

    //ToDo Send OK message to senderOfMessage

    if (!holdingElection) {

      initiateElection();

    }

  }

  //This is called whenever the node recovers from a crash in order to learn who the leader is.
  public void onRecoveryFromCrash() {

    initiateElection();

  }


}
