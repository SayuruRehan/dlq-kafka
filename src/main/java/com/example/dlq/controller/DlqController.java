package com.example.dlq.controller;

import com.example.dlq.entity.DlqMessage;
import com.example.dlq.service.DlqService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/dlq")
@CrossOrigin(origins = "*")
public class DlqController {

    private final DlqService dlqService;

    public DlqController(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    @GetMapping
    public ResponseEntity<Page<DlqMessage>> getDlqMessages(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        
        Pageable pageable = PageRequest.of(page, size);
        Page<DlqMessage> messages = dlqService.getDlqMessages(pageable);
        return ResponseEntity.ok(messages);
    }

    @GetMapping("/{id}")
    public ResponseEntity<DlqMessage> getDlqMessage(@PathVariable Long id) {
        Optional<DlqMessage> message = dlqService.getDlqMessage(id);
        return message.map(ResponseEntity::ok)
                     .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/key/{messageKey}")
    public ResponseEntity<List<DlqMessage>> getDlqMessagesByKey(@PathVariable String messageKey) {
        List<DlqMessage> messages = dlqService.getDlqMessagesByKey(messageKey);
        return ResponseEntity.ok(messages);
    }

    @PostMapping("/{id}/requeue")
    public ResponseEntity<String> requeueMessage(
            @PathVariable Long id,
            @RequestParam(defaultValue = "admin") String requeuedBy) {
        
        boolean success = dlqService.requeueMessage(id, requeuedBy);
        
        if (success) {
            return ResponseEntity.ok("Message requeued successfully");
        } else {
            return ResponseEntity.badRequest().body("Failed to requeue message");
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> deleteDlqMessage(@PathVariable Long id) {
        dlqService.deleteDlqMessage(id);
        return ResponseEntity.ok("Message deleted successfully");
    }

    @GetMapping("/requeueable")
    public ResponseEntity<Page<DlqMessage>> getRequeueableMessages(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        
        Pageable pageable = PageRequest.of(page, size);
        Page<DlqMessage> messages = dlqService.getRequeueableMessages(pageable);
        return ResponseEntity.ok(messages);
    }
}
