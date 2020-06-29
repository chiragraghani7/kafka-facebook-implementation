package com.javatechie.social.api;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.social.connect.ConnectionRepository;
import org.springframework.social.facebook.api.Facebook;
import org.springframework.social.facebook.api.PagedList;
import org.springframework.social.facebook.api.Post;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;

@Controller
@RequestMapping("/")
public class FacebookController {

	private Facebook facebook;

	private ConnectionRepository connectionRepository;


	@Autowired
	FacebookDemo facebookDemo;

	public FacebookController(Facebook facebook, ConnectionRepository connectionRepository) {
		this.facebook = facebook;
		this.connectionRepository = connectionRepository;
	}

	@GetMapping("/feeds")
	public String getFeeds(Model model) {
		if (connectionRepository.findPrimaryConnection(Facebook.class) == null) {
			return "redirect:/connect/facebook";
		}
		String str = facebookDemo.getFacebookObject();
		return "profile";
	}

	@GetMapping("/savePostData")
	public String savePostData(Model model) throws IOException {
		if (connectionRepository.findPrimaryConnection(Facebook.class) == null) {
			return "redirect:/connect/facebook";
		}
		String str = facebookDemo.elasticSearch();
		return "profile";
	}

	@GetMapping
	public String getfacebookFeeds(Model model) {
		if (connectionRepository.findPrimaryConnection(Facebook.class) == null) {
			return "redirect:/connect/facebook";
		}
		PagedList<Post> posts = facebook.feedOperations().getPosts();
		model.addAttribute("profileName", posts.get(0).getFrom().getName());
		model.addAttribute("posts", posts);
		return "profile";
	}
}
