import 'package:flutter/material.dart';
import 'screens/login_page.dart';
import 'screens/chat_screen.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Agentic AI Support',
      theme: ThemeData(primarySwatch: Colors.blue),
      home: const LoginPage(),
      routes: {
        '/chat': (context) => const ChatScreen(),
      },
    );
  }
}
