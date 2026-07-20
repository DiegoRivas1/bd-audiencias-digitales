package com.epcc.audiencias;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Representa cualquier evento (de user-events o purchase-events).
 * Los campos que no aplican a un tipo de evento quedan simplemente null
 * (ej. product/category/price en LOGIN o LOGOUT).
 */
public class Evento implements Serializable {

    public String event;       // LOGIN, SEARCH, VIEW_PRODUCT, ADD_CART, REMOVE_CART, LOGOUT, PURCHASE, PAYMENT_FAILED
    public String product;
    public String category;
    public String city;
    public Double price;
    public String timestamp;
    public String user_id;
    public String company;
    public String agent_id;
    public String agent_type;

    public Evento() {
    }

    public LocalDateTime getTimestampParsed() {
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public boolean esEventoCompra() {
        return "PURCHASE".equals(event) || "PAYMENT_FAILED".equals(event);
    }

    @Override
    public String toString() {
        return "Evento{" +
                "event='" + event + '\'' +
                ", product='" + product + '\'' +
                ", category='" + category + '\'' +
                ", city='" + city + '\'' +
                ", price=" + price +
                ", user_id='" + user_id + '\'' +
                ", agent_type='" + agent_type + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
