package org.apache.kafka.clients.consumer.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackgroundStateMachine {
    private static final Logger log = LoggerFactory.getLogger(BackgroundStateMachine.class);
    private BackgroundStates currentState;

    public BackgroundStateMachine(BackgroundStates initialState) {
        this.currentState = initialState;
    }

    public boolean transitionTo(BackgroundStates nextState) {
        if(validateStateTransition(currentState, nextState)) {
            this.currentState = nextState;
        }
        return false;
    }

    @Override
    public String toString() {
        return this.currentState.toString();
    }

    public boolean isStable() {
        return this.currentState == BackgroundStates.STABLE;
    }

    private boolean validateStateTransition(BackgroundStates currentState, BackgroundStates nextState) {
        switch(currentState) {
            case DOWN:
                if(nextState != BackgroundStates.INITIALIZED) {
                    log.error("INITIALIZED is not a possible next state");
                    return false;
                }
                break;
            case INITIALIZED:
                if(nextState == BackgroundStates.STABLE) {
                    return false;
                }
                break;
            case COORDINATOR_DISCOVERY:
            case STABLE:
                break;
            default:
                return false;
        }
        return true;
    }

    public void maybeTransitionToNextState() {
        return; // TODO
    }

    public BackgroundStates getCurrentState() {
        return currentState;
    }
}
