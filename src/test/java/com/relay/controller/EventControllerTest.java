package com.relay.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.event.RelayEvent;
import com.relay.producer.RelayEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private RelayEventProducer eventProducer;

    @Test
    void postEvents_returns202Accepted_andPublishesEvent() throws Exception {
        RelayEvent event = new RelayEvent("evt-1001", "agg-42", "OrderCreated", "{\"amount\":99.99}");
        when(eventProducer.sendEvent(any(RelayEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        mockMvc.perform(post("/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isAccepted());

        verify(eventProducer).sendEvent(event);
    }
}
