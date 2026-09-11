import 'dart:io';
import 'package:flutter/material.dart';
import 'package:image_picker/image_picker.dart';
import '../services/api_service.dart';

class ChatScreen extends StatefulWidget {
  const ChatScreen({super.key});

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final TextEditingController _controller = TextEditingController();
  final ScrollController _scrollController = ScrollController();
  final ImagePicker _picker = ImagePicker();

  String currentChatId = "chat_1";

  Map<String, List<Map<String, dynamic>>> chatSessions = {
    "chat_1": [],
  };
  Future<void> sendMessage({String? text, File? image}) async {
    if ((text == null || text.isEmpty) && image == null) return;

    setState(() {
      chatSessions[currentChatId]!.add({
        "sender": "user",
        "text": text,
        "image": image,
      });

      chatSessions[currentChatId]!.add({
        "sender": "ai",
        "text": "AI is thinking...",
      });
    });

    _controller.clear();
    _scrollToBottom();

    try {
      final response = await ApiService.sendMessage(text ?? "");

      setState(() {
        chatSessions[currentChatId]!.last["text"] = response["answer"];
        chatSessions[currentChatId]!.last["documents"] = response["documents"];
      });
    } catch (e) {
      setState(() {
        chatSessions[currentChatId]!.last["text"] = "Error connecting to AI";
        chatSessions[currentChatId]!.last["documents"] = [];
      });
    }

    _scrollToBottom();
  }
  Future<void> pickImage() async {
    final picked = await _picker.pickImage(source: ImageSource.gallery);
    if (picked != null) {
      sendMessage(image: File(picked.path));
    }
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 300),
          curve: Curves.easeOut,
        );
      }
    });
  }

  void createNewChat() {
    final newId = "chat_${chatSessions.length + 1}";
    setState(() {
      chatSessions[newId] = [];
      currentChatId = newId;
    });
    Navigator.pop(context);
  }

  @override
  Widget build(BuildContext context) {
    final messages = chatSessions[currentChatId]!;

    return Scaffold(
      appBar: AppBar(title: const Text("AI Support Help Desk")),
      drawer: Drawer(
        child: Column(
          children: [
            const DrawerHeader(
              child: Text(
                "Chats",
                style: TextStyle(fontSize: 22, fontWeight: FontWeight.bold),
              ),
            ),
            Expanded(
              child: ListView(
                children: chatSessions.keys.map((id) {
                  return ListTile(
                    title: Text("Session ${id.split('_').last}"),
                    selected: id == currentChatId,
                    onTap: () {
                      setState(() => currentChatId = id);
                      Navigator.pop(context);
                    },
                  );
                }).toList(),
              ),
            ),
            ListTile(
              leading: const Icon(Icons.add),
              title: const Text("New Chat"),
              onTap: createNewChat,
            ),
          ],
        ),
      ),
      body: Column(
        children: [
          Expanded(
            child: ListView.builder(
              controller: _scrollController,
              itemCount: messages.length,
              padding: const EdgeInsets.all(12),
              itemBuilder: (context, index) {
                final msg = messages[index];
                final isUser = msg["sender"] == "user";

                return Align(
                  alignment:
                  isUser ? Alignment.centerRight : Alignment.centerLeft,
                  child: Container(
                    margin: const EdgeInsets.symmetric(vertical: 6),
                    padding: const EdgeInsets.all(12),
                    constraints: const BoxConstraints(maxWidth: 300),
                    decoration: BoxDecoration(
                      color: isUser ? Colors.blue[300] : Colors.grey[300],
                      borderRadius: BorderRadius.circular(12),
                    ),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        if (msg["image"] != null)
                          ClipRRect(
                            borderRadius: BorderRadius.circular(8),
                            child: Image.file(
                              msg["image"],
                              height: 150,
                            ),
                          ),
                        if (msg["text"] != null)
                          Text(
                            msg["text"],
                            style: const TextStyle(fontSize: 18),
                          ),
                        if (msg["documents"] != null &&
                            (msg["documents"] as List).isNotEmpty)
                          Padding(
                            padding: const EdgeInsets.only(top: 8),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: (msg["documents"] as List)
                                  .map((doc) => Text(
                                "- ${doc['file']} (${doc['product']})",
                                style: const TextStyle(
                                    fontSize: 12,
                                    color: Colors.black54),
                              ))
                                  .toList(),
                            ),
                          ),
                      ],
                    ),
                  ),
                );
              },
            ),
          ),

          Container(
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
            decoration: BoxDecoration(
              color: Colors.white,
              boxShadow: [
                BoxShadow(color: Colors.black12, blurRadius: 4),
              ],
            ),
            child: Row(
              children: [
                IconButton(
                  icon: const Icon(Icons.image),
                  onPressed: pickImage,
                ),
                Expanded(
                  child: TextField(
                    controller: _controller,
                    minLines: 1,
                    maxLines: 4,
                    decoration: InputDecoration(
                      hintText: "Ask something...",
                      filled: true,
                      fillColor: Colors.grey[100],
                      border: OutlineInputBorder(
                        borderRadius: BorderRadius.circular(24),
                        borderSide: BorderSide.none,
                      ),
                      contentPadding: const EdgeInsets.symmetric(
                        horizontal: 16,
                        vertical: 14,
                      ),
                    ),
                  ),
                ),
                const SizedBox(width: 8),
                CircleAvatar(
                  backgroundColor: Colors.blue,
                  child: IconButton(
                    icon: const Icon(Icons.send, color: Colors.white),
                    onPressed: () =>
                        sendMessage(text: _controller.text.trim()),
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}
