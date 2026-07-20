package com.epcc.audiencias;

import java.io.Serializable;

public class AudienciaResultado implements Serializable {
    public String user_id;
    public String agent_type;
    public String audiencias;   // lista de audiencias unidas por " | "
    public String timestamp;

    public AudienciaResultado() {
    }

    public AudienciaResultado(String user_id, String agent_type, String audiencias, String timestamp) {
        this.user_id = user_id;
        this.agent_type = agent_type;
        this.audiencias = audiencias;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("Usuario %-10s (%-20s) -> %s", user_id, agent_type, audiencias);
    }
}
