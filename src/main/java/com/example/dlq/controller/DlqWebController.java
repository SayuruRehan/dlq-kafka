package com.example.dlq.controller;

import com.example.dlq.entity.DlqMessage;
import com.example.dlq.service.DlqService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/dlq")
public class DlqWebController {

    private final DlqService dlqService;

    public DlqWebController(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    @GetMapping
    public String dlqDashboard(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            Model model) {
        
        Pageable pageable = PageRequest.of(page, size);
        Page<DlqMessage> messages = dlqService.getDlqMessages(pageable);
        
        model.addAttribute("messages", messages);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", messages.getTotalPages());
        model.addAttribute("totalElements", messages.getTotalElements());
        
        return "dlq-dashboard";
    }

    @GetMapping("/{id}")
    public String messageDetail(@PathVariable Long id, Model model) {
        DlqMessage message = dlqService.getDlqMessage(id).orElse(null);
        model.addAttribute("message", message);
        return "message-detail";
    }

    @PostMapping("/{id}/requeue")
    public String requeueMessage(
            @PathVariable Long id,
            @RequestParam(defaultValue = "admin") String requeuedBy) {
        
        dlqService.requeueMessage(id, requeuedBy);
        return "redirect:/dlq";
    }

    @PostMapping("/{id}/delete")
    public String deleteMessage(@PathVariable Long id) {
        dlqService.deleteDlqMessage(id);
        return "redirect:/dlq";
    }
}
