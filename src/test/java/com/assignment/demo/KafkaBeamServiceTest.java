package com.assignment.demo;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;

import com.assignment.service.KafkaBeamService;
import com.assignment.service.KafkaBeamService.EvenLengthFilterFn;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest
public class KafkaBeamServiceTest {
	
	@InjectMocks
    private KafkaBeamService kafkaBeamService;

    @Mock
    private DoFn.OutputReceiver<String> outputReceiver;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testEvenAge() throws Exception {
        // Arrange
        EvenLengthFilterFn fn = new EvenLengthFilterFn();
        OutputReceiver<String> outputReceiver = mock(OutputReceiver.class);
        
        // Create a valid JSON string representing a Person
        String jsonMessage = "{\"dateOfBirth\":\"2000-01-01\"}"; // Assuming this date gives an even age
        
        // Act
        fn.processElement(jsonMessage, outputReceiver);
        
        // Capture the output
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(outputReceiver).output(captor.capture());
        
        // Assert
        String output = captor.getValue();
        assertTrue(output.contains("Age is even"));
    }

}
