import 'package:flutter/material.dart';
import 'chat_screen.dart';

class SupportPage extends StatelessWidget {
  const SupportPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Support'),
      ),
      body: Column(
        children: [
          Container(
            height: 40,
            color: Colors.blue.shade200,
            alignment: Alignment.centerLeft,
            padding: const EdgeInsets.symmetric(horizontal: 10),
            child: const Text(
              ' LLM Coming Soon ',
              style: TextStyle(fontSize: 16),
            ),
          ),
          const Expanded(
            child: ChatScreen(),
          ),
        ],
      ),
    );
  }
}
