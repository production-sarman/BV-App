import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:url_launcher/url_launcher.dart';
import '../services/api_service.dart';


class _AppColors {
  static const background     = Color(0xFFF5F6FA);
  static const surface        = Color(0xFFFFFFFF);
  static const primary        = Color(0xFF2563EB);
  static const primaryLight   = Color(0xFFEFF6FF);
  static const primaryDark    = Color(0xFF1D4ED8);
  static const userBubble     = Color(0xFF2563EB);
  static const botBubble      = Color(0xFFFFFFFF);
  static const accent         = Color(0xFF10B981);
  static const warning        = Color(0xFFF59E0B);
  static const textPrimary    = Color(0xFF111827);
  static const textSecondary  = Color(0xFF6B7280);
  static const textLight      = Color(0xFF9CA3AF);
  static const border         = Color(0xFFE5E7EB);
  static const shadow         = Color(0x0D000000);
  static const drawerBg       = Color(0xFF1E293B);
  static const drawerSurface  = Color(0xFF334155);
  static const drawerSelected = Color(0xFF2563EB);
  static const drawerText     = Color(0xFFCBD5E1);
  static const drawerTextBold = Color(0xFFF1F5F9);
}
class VSM_Assistant extends StatefulWidget {
  const VSM_Assistant({Key? key}) : super(key: key);

  @override
  State<VSM_Assistant> createState() => _VSM_AssistantState();
}

class _VSM_AssistantState extends State<VSM_Assistant>
    with TickerProviderStateMixin {
  static const String storageKey = "chat_session";

  List<Map<String, dynamic>> _sessions = [];
  int _currentIndex = 0;
  bool _isLoading = false;
  bool _isFirstOpen = true;
  bool _showScrollButton = false;

  final TextEditingController _controller = TextEditingController();
  final ScrollController _scrollController = ScrollController();
  final FocusNode _inputFocusNode = FocusNode();

  final List<AnimationController> _bubbleAnimControllers = [];

  @override
  void initState() {
    super.initState();
    _loadSessions();
    _scrollController.addListener(() {
      final maxScroll     = _scrollController.position.maxScrollExtent;
      final currentScroll = _scrollController.position.pixels;
      final difference    = maxScroll - currentScroll;
      setState(() {
        _showScrollButton = difference > 200;
      });
    });
  }

  @override
  void dispose() {
    _controller.dispose();
    _scrollController.dispose();
    _inputFocusNode.dispose();
    for (final c in _bubbleAnimControllers) {
      c.dispose();
    }
    super.dispose();
  }
  Future<void> _loadSessions() async {
    final prefs = await SharedPreferences.getInstance();
    final data  = prefs.getString(storageKey);

    if (data != null) {
      final decoded = List<Map<String, dynamic>>.from(jsonDecode(data));
      setState(() {
        _sessions = decoded;
        if (_sessions.length >= 15) _sessions.removeAt(0);
        _sessions.add({
          "title"   : "New Chat ${_sessions.length + 1}",
          "messages": <dynamic>[],
        });
        _currentIndex = _sessions.length - 1;
        _isFirstOpen  = false;
      });
    } else {
      _createNewChat(silent: true);
    }
    _saveSessions();
  }

  Future<void> _saveSessions() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(storageKey, jsonEncode(_sessions));
  }
  void _createNewChat({bool silent = false}) {
    setState(() {
      if (_sessions.length >= 15) _sessions.removeAt(0);
      _sessions.add({
        "title"   : "New Chat ${_sessions.length + 1}",
        "messages": <dynamic>[],
      });
      _currentIndex = _sessions.length - 1;
      _isFirstOpen  = false;
    });
    _saveSessions();
    if (!silent) Navigator.pop(context);
  }

  void _handleFirstOpenTap() {
    if (!_isFirstOpen) return;
    setState(() {
      if (_sessions.length >= 15) _sessions.removeAt(0);
      _sessions.add({
        "title"   : "New Chat ${_sessions.length + 1}",
        "messages": <dynamic>[],
      });
      _currentIndex = _sessions.length - 1;
      _isFirstOpen  = false;
    });
    _saveSessions();
  }
  void _renameChat() {
    final renameController =
    TextEditingController(text: _sessions[_currentIndex]["title"]);

    showDialog(
      context: context,
      builder: (_) =>
          AlertDialog(
            shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(20)),
            title: const Text("Rename Chat",
                style: TextStyle(
                    fontWeight: FontWeight.w700,
                    color: _AppColors.textPrimary)),
            content: TextField(
              controller: renameController,
              autofocus: true,
              decoration: InputDecoration(
                hintText: "Enter new chat name",
                hintStyle: const TextStyle(color: _AppColors.textLight),
                border: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(10),
                    borderSide: const BorderSide(color: _AppColors.border)),
                focusedBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(10),
                    borderSide:
                    const BorderSide(color: _AppColors.primary, width: 2)),
                contentPadding:
                const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
              ),
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context),
                child: const Text("Cancel",
                    style: TextStyle(color: _AppColors.textSecondary)),
              ),
              ElevatedButton(
                style: ElevatedButton.styleFrom(
                  backgroundColor: _AppColors.primary,
                  shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8)),
                ),
                onPressed: () {
                  setState(() {
                    _sessions[_currentIndex]["title"] =
                    renameController.text
                        .trim()
                        .isEmpty
                        ? "Untitled Chat"
                        : renameController.text.trim();
                  });
                  _saveSessions();
                  Navigator.pop(context);
                },
                child:
                const Text("Save", style: TextStyle(color: Colors.white)),
              ),
            ],
          ),
    );
  }
  void _deleteSession(int index) {
    setState(() {
      _sessions.removeAt(index);
      if (_sessions.isEmpty) {
        _sessions.add({"title": "New Chat 1", "messages": <dynamic>[]});
        _currentIndex = 0;
      } else if (_currentIndex >= _sessions.length) {
        _currentIndex = _sessions.length - 1;
      }
    });
    _saveSessions();
    Navigator.pop(context);
  }

  List<dynamic> get _messages => _sessions[_currentIndex]["messages"];

  void _scrollToBottom() {
    Future.delayed(const Duration(milliseconds: 120), () {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 350),
          curve: Curves.easeOut,
        );
      }
    });
  }

  String _formatQuery(String text) {
    final userFormat =
    RegExp(r'([\w\-]+)\s+stage\s+(\d+\.\d+)', caseSensitive: false);
    final match = userFormat.firstMatch(text);
    if (match != null) {
      return "stage ${match.group(2)} for ${match.group(1)}";
    }
    return text;
  }
  bool _isNotFoundResponse(Map<String, dynamic> result) {
    final answer      = (result["answer"]      ?? "").toString().trim();
    final document    = (result["document"]    ?? "").toString().trim();
    final explanation = (result["explanation"] ?? "").toString().trim();

    if (answer.isEmpty && document.isEmpty && explanation.isEmpty) return true;
    if (result["found"] == false) return true;

    final notFoundPhrases = [
      "no data",
      "not found",
      "no result",
      "does not exist",
      "couldn't find",
      "could not find",
      "unavailable",
    ];
    final lower = answer.toLowerCase();
    if (notFoundPhrases.any((p) => lower.contains(p))) return true;

    return false;
  }

  Future<void> sendMessage(String text) async {
    if (text.trim().isEmpty || _isLoading) return;

    if (_isFirstOpen) _handleFirstOpenTap();

    final formattedQuery = _formatQuery(text.trim());

    if ((_messages as List).isEmpty) {
      final snippet = text.trim().length > 28
          ? "${text.trim().substring(0, 28)}…"
          : text.trim();
      setState(() => _sessions[_currentIndex]["title"] = snippet);
    }

    setState(() {
      (_messages as List).add({"sender": "user", "text": text});
      _isLoading = true;
    });

    _controller.clear();
    _scrollToBottom();
    await _saveSessions();

    try {
      final Map<String, dynamic> result =
      await ApiService.sendMessage(formattedQuery);

      final bool notFound = _isNotFoundResponse(result);

      setState(() {
        (_messages as List).add(notFound
            ? {
          "sender"     : "bot",
          "not_found"  : true,
          "answer"     : "",
          "document"   : "",
          "preview_url": "",
          "explanation": "",
        }
            : {
          "sender"     : "bot",
          "not_found"  : false,
          "answer"     : result["answer"]      ?? "",
          "document"   : result["document"]    ?? "",
          "preview_url": result["preview_url"] ?? "",
          "explanation": result["explanation"] ?? "",
        });
        _isLoading = false;
      });
    } catch (e) {
      setState(() {
        (_messages as List).add({
          "sender"     : "bot",
          "not_found"  : false,
          "answer"     : "Something went wrong. Please try again.\n\n${e.toString()}",
          "document"   : "",
          "preview_url": "",
          "explanation": "",
        });
        _isLoading = false;
      });
    }

    _scrollToBottom();
    await _saveSessions();
  }
  void _copyToClipboard(String text) {
    Clipboard.setData(ClipboardData(text: text));
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: const Row(
          children: [
            Icon(Icons.check_circle_outline, color: Colors.white, size: 18),
            SizedBox(width: 8),
            Text("Copied to clipboard"),
          ],
        ),
        backgroundColor: _AppColors.accent,
        behavior: SnackBarBehavior.floating,
        shape:
        RoundedRectangleBorder(borderRadius: BorderRadius.circular(10)),
        duration: const Duration(seconds: 2),
      ),
    );
  }

  Future<void> _openUrl(String url) async {
    if (url.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: const Text("No document URL available"),
          backgroundColor: Colors.red.shade400,
          behavior: SnackBarBehavior.floating,
          shape:
          RoundedRectangleBorder(borderRadius: BorderRadius.circular(10)),
        ),
      );
      return;
    }
    final uri = Uri.parse(url);
    if (await canLaunchUrl(uri)) {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    } else {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: const Text("Could not open the document"),
          backgroundColor: Colors.red.shade400,
          behavior: SnackBarBehavior.floating,
          shape:
          RoundedRectangleBorder(borderRadius: BorderRadius.circular(10)),
        ),
      );
    }
  }


  Widget _buildMessageBubble(Map<String, dynamic> message, int index) {
    final isUser = message["sender"] == "user";
    if (isUser) return _buildUserBubble(message);
    final isNotFound = message["not_found"] == true;
    if (isNotFound) return _buildNotFoundBubble();
    return _buildBotBubble(message);
  }

  Widget _buildUserBubble(Map<String, dynamic> msg) {
    final text = msg["text"] ?? "";
    return Padding(
      padding: const EdgeInsets.only(bottom: 12, left: 48),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.end,
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          Flexible(
            child: Container(
              padding:
              const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
              decoration: BoxDecoration(
                color: _AppColors.userBubble,
                borderRadius: const BorderRadius.only(
                  topLeft    : Radius.circular(18),
                  topRight   : Radius.circular(18),
                  bottomLeft : Radius.circular(18),
                  bottomRight: Radius.circular(4),
                ),
                boxShadow: [
                  BoxShadow(
                    color     : _AppColors.primary.withOpacity(0.25),
                    blurRadius: 8,
                    offset    : const Offset(0, 3),
                  ),
                ],
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.end,
                children: [
                  SelectableText(
                    text,
                    style: const TextStyle(
                        fontSize: 15, color: Colors.white, height: 1.45),
                  ),
                  const SizedBox(height: 6),
                  GestureDetector(
                    onTap: () => _copyToClipboard(text),
                    child: const Icon(Icons.copy_rounded,
                        size: 13, color: Colors.white54),
                  ),
                ],
              ),
            ),
          ),
          const SizedBox(width: 8),
          CircleAvatar(
            radius: 16,
            backgroundColor: _AppColors.primary.withOpacity(0.15),
            child: const Icon(Icons.person_rounded,
                size: 18, color: _AppColors.primary),
          ),
        ],
      ),
    );
  }

  Widget _buildNotFoundBubble() {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12, right: 48),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          _botAvatar(),
          const SizedBox(width: 8),
          Flexible(
            child: Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: const Color(0xFFFFFBEB),
                borderRadius: const BorderRadius.only(
                  topLeft    : Radius.circular(18),
                  topRight   : Radius.circular(18),
                  bottomRight: Radius.circular(18),
                  bottomLeft : Radius.circular(4),
                ),
                border: Border.all(
                    color: _AppColors.warning.withOpacity(0.4), width: 1),
                boxShadow: [
                  BoxShadow(
                    color     : Colors.black.withOpacity(0.05),
                    blurRadius: 8,
                    offset    : const Offset(0, 3),
                  ),
                ],
              ),
              child: Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Container(
                    padding: const EdgeInsets.all(8),
                    decoration: BoxDecoration(
                      color        : _AppColors.warning.withOpacity(0.15),
                      borderRadius : BorderRadius.circular(10),
                    ),
                    child: const Icon(Icons.search_off_rounded,
                        color: _AppColors.warning, size: 22),
                  ),
                  const SizedBox(width: 12),
                  const Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          "Product Not Found",
                          style: TextStyle(
                            fontWeight: FontWeight.w700,
                            fontSize  : 15,
                            color     : Color(0xFF92400E),
                          ),
                        ),
                        SizedBox(height: 4),
                        SelectableText(
                          "We currently don't have data for this product variant, or the details you entered may be incorrect.\n\nPlease double-check the product code and stage number and try again.",
                          style: TextStyle(
                            fontSize: 13.5,
                            color   : Color(0xFF78350F),
                            height  : 1.5,
                          ),
                        ),
                      ],
                    ),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildBotBubble(Map<String, dynamic> msg) {
    final answer      = msg["answer"]      ?? "";
    final explanation = msg["explanation"] ?? "";
    final document    = msg["document"]    ?? "";
    final previewUrl  = msg["preview_url"] ?? "";
    final fullText    =
    [answer, explanation].where((s) => s.isNotEmpty).join("\n\n");

    return Padding(
      padding: const EdgeInsets.only(bottom: 12, right: 48),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          _botAvatar(),
          const SizedBox(width: 8),
          Flexible(
            child: Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: _AppColors.botBubble,
                borderRadius: const BorderRadius.only(
                  topLeft    : Radius.circular(18),
                  topRight   : Radius.circular(18),
                  bottomRight: Radius.circular(18),
                  bottomLeft : Radius.circular(4),
                ),
                border: Border.all(color: _AppColors.border, width: 1),
                boxShadow: [
                  BoxShadow(
                    color     : Colors.black.withOpacity(0.05),
                    blurRadius: 8,
                    offset    : const Offset(0, 3),
                  ),
                ],
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [

                  if (answer.isNotEmpty)
                    SelectableText(
                      answer,
                      style: const TextStyle(
                          fontSize: 15,
                          color   : _AppColors.textPrimary,
                          height  : 1.55),
                    ),

                  if (explanation.isNotEmpty) ...[
                    const SizedBox(height: 12),
                    Container(
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color        : _AppColors.primaryLight,
                        borderRadius : BorderRadius.circular(10),
                        border       : Border.all(
                            color: _AppColors.primary.withOpacity(0.15)),
                      ),
                      child: Row(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          const Icon(Icons.lightbulb_outline_rounded,
                              size: 18, color: _AppColors.primary),
                          const SizedBox(width: 8),
                          Expanded(
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                const Text("Explanation",
                                    style: TextStyle(
                                      fontWeight: FontWeight.w700,
                                      fontSize  : 13,
                                      color     : _AppColors.primary,
                                    )),
                                const SizedBox(height: 4),
                                SelectableText(
                                  explanation,
                                  style: const TextStyle(
                                    fontSize: 13.5,
                                    color   : _AppColors.textPrimary,
                                    height  : 1.5,
                                  ),
                                ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    ),
                  ],

                  if (document.isNotEmpty) ...[
                    const SizedBox(height: 12),
                    Container(
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color        : const Color(0xFFF0FDF4),
                        borderRadius : BorderRadius.circular(10),
                        border       : Border.all(
                            color: _AppColors.accent.withOpacity(0.2)),
                      ),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Row(children: [
                            const Icon(Icons.insert_drive_file_rounded,
                                size: 16, color: _AppColors.accent),
                            const SizedBox(width: 6),
                            const Text("Document",
                                style: TextStyle(
                                  fontWeight: FontWeight.w700,
                                  fontSize  : 13,
                                  color     : _AppColors.accent,
                                )),
                          ]),
                          const SizedBox(height: 10),

                          SizedBox(
                            width: double.infinity,
                            child: ElevatedButton.icon(
                              icon : const Icon(Icons.download_rounded,
                                  size: 16),
                              label: const Text("Download Document",
                                  style: TextStyle(
                                      fontWeight: FontWeight.w600,
                                      fontSize  : 13)),
                              style: ElevatedButton.styleFrom(
                                backgroundColor: _AppColors.primary,
                                foregroundColor: Colors.white,
                                elevation      : 0,
                                padding        : const EdgeInsets.symmetric(
                                    vertical: 10),
                                shape: RoundedRectangleBorder(
                                    borderRadius: BorderRadius.circular(8)),
                              ),
                              onPressed: () => _openUrl(document),
                            ),
                          ),

                          if (previewUrl.isNotEmpty) ...[
                            const SizedBox(height: 6),
                            SizedBox(
                              width: double.infinity,
                              child: OutlinedButton.icon(
                                icon : const Icon(Icons.visibility_rounded,
                                    size: 16),
                                label: const Text("Preview Document",
                                    style: TextStyle(
                                        fontWeight: FontWeight.w600,
                                        fontSize  : 13)),
                                style: OutlinedButton.styleFrom(
                                  foregroundColor: _AppColors.primary,
                                  side   : const BorderSide(
                                      color: _AppColors.primary),
                                  padding: const EdgeInsets.symmetric(
                                      vertical: 10),
                                  shape: RoundedRectangleBorder(
                                      borderRadius:
                                      BorderRadius.circular(8)),
                                ),
                                onPressed: () =>
                                    _openUrl(previewUrl),
                              ),
                            ),
                          ],
                        ],
                      ),
                    ),
                  ],

                  if (fullText.isNotEmpty) ...[
                    const SizedBox(height: 10),
                    Align(
                      alignment: Alignment.centerRight,
                      child: GestureDetector(
                        onTap: () => _copyToClipboard(fullText),
                        child: Container(
                          padding: const EdgeInsets.symmetric(
                              horizontal: 10, vertical: 5),
                          decoration: BoxDecoration(
                            color        : _AppColors.background,
                            borderRadius : BorderRadius.circular(8),
                            border       : Border.all(
                                color: _AppColors.border),
                          ),
                          child: const Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              Icon(Icons.copy_rounded,
                                  size : 12,
                                  color: _AppColors.textSecondary),
                              SizedBox(width: 4),
                              Text("Copy",
                                  style: TextStyle(
                                      fontSize  : 11,
                                      color     : _AppColors.textSecondary,
                                      fontWeight: FontWeight.w500)),
                            ],
                          ),
                        ),
                      ),
                    ),
                  ],
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _botAvatar() {
    return Container(
      width : 26,
      height: 26,
      decoration: BoxDecoration(
        gradient: const LinearGradient(
          colors: [_AppColors.primary, _AppColors.primaryDark],
          begin : Alignment.topLeft,
          end   : Alignment.bottomRight,
        ),
        borderRadius: BorderRadius.circular(10),
      ),
      child: const Center(
        child: Text("BV",
            style: TextStyle(
                color      : Colors.white,
                fontWeight : FontWeight.w800,
                fontSize   : 16)),
      ),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Container(
            width : 80,
            height: 80,
            decoration: BoxDecoration(
              gradient: const LinearGradient(
                colors: [_AppColors.primary, _AppColors.primaryDark],
                begin : Alignment.topLeft,
                end   : Alignment.bottomRight,
              ),
              borderRadius: BorderRadius.circular(24),
              boxShadow: [
                BoxShadow(
                  color     : _AppColors.primary.withOpacity(0.3),
                  blurRadius: 20,
                  offset    : const Offset(0, 8),
                ),
              ],
            ),
            child: const Center(
              child: Text("V",
                  style: TextStyle(
                      color     : Colors.white,
                      fontWeight: FontWeight.w900,
                      fontSize  : 36)),
            ),
          ),
          const SizedBox(height: 20),
          const Text("VSM Assistant",
              style: TextStyle(
                  fontSize  : 22,
                  fontWeight: FontWeight.w800,
                  color     : _AppColors.textPrimary)),
          const SizedBox(height: 8),
          const Text(
            "Ask about any product code and stage",
            style: TextStyle(fontSize: 14, color: _AppColors.textSecondary),
          ),
          const SizedBox(height: 24),
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
            decoration: BoxDecoration(
              color        : _AppColors.primaryLight,
              borderRadius : BorderRadius.circular(10),
              border       : Border.all(
                  color: _AppColors.primary.withOpacity(0.2)),
            ),
            child: const Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(Icons.tips_and_updates_rounded,
                    size: 16, color: _AppColors.primary),
                SizedBox(width: 8),
                Text(
                  "e.g.  TR_3PH_CS_01_9V_4000A stage 1.3",
                  style: TextStyle(
                      fontSize  : 13,
                      color     : _AppColors.primary,
                      fontWeight: FontWeight.w500),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildTypingIndicator() {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12, right: 48),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          _botAvatar(),
          const SizedBox(width: 8),
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 14),
            decoration: BoxDecoration(
              color: _AppColors.botBubble,
              borderRadius: const BorderRadius.only(
                topLeft    : Radius.circular(18),
                topRight   : Radius.circular(18),
                bottomRight: Radius.circular(18),
                bottomLeft : Radius.circular(4),
              ),
              border: Border.all(color: _AppColors.border),
              boxShadow: [
                BoxShadow(
                  color     : Colors.black.withOpacity(0.05),
                  blurRadius: 8,
                  offset    : const Offset(0, 3),
                ),
              ],
            ),
            child: const Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                _TypingDot(delay: 1),
                SizedBox(width: 5),
                _TypingDot(delay: 160),
                SizedBox(width: 5),
                _TypingDot(delay: 320),
              ],
            ),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final hasMessages = _sessions.isNotEmpty &&
        (_sessions[_currentIndex]["messages"] as List).isNotEmpty;

    return Scaffold(
      backgroundColor: _AppColors.background,
      appBar: AppBar(
        backgroundColor      : _AppColors.surface,
        elevation            : 0,
        scrolledUnderElevation: 1,
        shadowColor          : _AppColors.border,
        leading: Row(
          children: [
            IconButton(
              icon: const Icon(Icons.arrow_back_rounded,
                  color: _AppColors.textPrimary),
              onPressed: () => Navigator.pop(context),
            ),
            Builder(
              builder: (ctx) => IconButton(
                icon: const Icon(Icons.menu_rounded,
                    color: _AppColors.textPrimary),
                onPressed: () => Scaffold.of(ctx).openDrawer(),
              ),
            ),
          ],
        ),
        leadingWidth: 96,
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              _sessions.isEmpty
                  ? "VSM Assistant"
                  : _sessions[_currentIndex]["title"],
              style: const TextStyle(
                  fontSize  : 16,
                  fontWeight: FontWeight.w700,
                  color     : _AppColors.textPrimary),
              overflow: TextOverflow.ellipsis,
            ),
            if (_isLoading)
              const Text("Thinking…",
                  style: TextStyle(
                      fontSize  : 11,
                      color     : _AppColors.primary,
                      fontWeight: FontWeight.w500)),
          ],
        ),
        actions: [
          IconButton(
            icon   : const Icon(Icons.edit_outlined,
                color: _AppColors.textSecondary, size: 20),
            onPressed: _renameChat,
            tooltip: "Rename chat",
          ),
          IconButton(
            icon   : const Icon(Icons.add_comment_outlined,
                color: _AppColors.textSecondary, size: 20),
            onPressed: () => _createNewChat(silent: true),
            tooltip: "New chat",
          ),
          const SizedBox(width: 4),
        ],
      ),
      drawer: _buildDrawer(),

      body: GestureDetector(
        onTap    : _isFirstOpen ? _handleFirstOpenTap : null,
        behavior : HitTestBehavior.translucent,
        child: Column(
          children: [
            const Divider(height: 1, color: _AppColors.border),
            Expanded(
              child: _sessions.isEmpty
                  ? _buildEmptyState()
                  : ListView.builder(
                controller: _scrollController,
                padding   : const EdgeInsets.fromLTRB(12, 16, 12, 8),
                itemCount : _messages.length + (_isLoading ? 1 : 0),
                itemBuilder: (context, index) {
                  if (!hasMessages && index == 0 && !_isLoading) {
                    return _buildEmptyState();
                  }
                  if (_isLoading && index == _messages.length) {
                    return _buildTypingIndicator();
                  }
                  return _buildMessageBubble(
                    Map<String, dynamic>.from(_messages[index]),
                    index,
                  );
                },
              ),
            ),
            _buildInputBar(),
          ],
        ),
      ),
    );
  }


  Widget _buildDrawer() {
    return Drawer(
      backgroundColor: _AppColors.drawerBg,
      child: Column(
        children: [
          Container(
            width  : double.infinity,
            padding: EdgeInsets.only(
              top   : MediaQuery.of(context).padding.top + 20,
              left  : 20,
              right : 20,
              bottom: 20,
            ),
            decoration: const BoxDecoration(
              color : _AppColors.drawerBg,
              border: Border(
                  bottom: BorderSide(color: _AppColors.drawerSurface)),
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Container(
                      width : 36,
                      height: 36,
                      decoration: BoxDecoration(
                        gradient: const LinearGradient(
                          colors: [
                            _AppColors.primary,
                            _AppColors.primaryDark
                          ],
                        ),
                        borderRadius: BorderRadius.circular(10),
                      ),
                      child: const Center(
                        child: Text("V",
                            style: TextStyle(
                                color     : Colors.white,
                                fontWeight: FontWeight.w800,
                                fontSize  : 18)),
                      ),
                    ),
                    const SizedBox(width: 10),
                    const Text("VSM Assistant",
                        style: TextStyle(
                            color     : _AppColors.drawerTextBold,
                            fontSize  : 17,
                            fontWeight: FontWeight.w700)),
                  ],
                ),
                const SizedBox(height: 16),
                SizedBox(
                  width: double.infinity,
                  child: ElevatedButton.icon(
                    icon : const Icon(Icons.add_rounded, size: 18),
                    label: const Text("New Chat",
                        style: TextStyle(
                            fontWeight: FontWeight.w600, fontSize: 14)),
                    style: ElevatedButton.styleFrom(
                      backgroundColor: _AppColors.primary,
                      foregroundColor: Colors.white,
                      elevation      : 0,
                      padding        : const EdgeInsets.symmetric(vertical: 10),
                      shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(10)),
                    ),
                    onPressed: () => _createNewChat(),
                  ),
                ),
              ],
            ),
          ),

          const Padding(
            padding: EdgeInsets.fromLTRB(16, 14, 16, 6),
            child: Align(
              alignment: Alignment.centerLeft,
              child: Text("Chats",
                  style: TextStyle(
                      color      : _AppColors.textLight,
                      fontSize   : 11,
                      fontWeight : FontWeight.w600,
                      letterSpacing: 1.1)),
            ),
          ),

          Expanded(
            child: ListView.builder(
              padding   : const EdgeInsets.symmetric(
                  horizontal: 10, vertical: 4),
              itemCount : _sessions.length,
              itemBuilder: (context, index) {
                final isSelected = index == _currentIndex;
                return Container(
                  margin: const EdgeInsets.only(bottom: 4),
                  decoration: BoxDecoration(
                    color: isSelected
                        ? _AppColors.drawerSelected.withOpacity(0.2)
                        : Colors.transparent,
                    borderRadius: BorderRadius.circular(10),
                    border: isSelected
                        ? Border.all(
                        color: _AppColors.primary.withOpacity(0.4),
                        width: 1)
                        : null,
                  ),
                  child: ListTile(
                    dense  : true,
                    leading: Icon(
                      Icons.chat_bubble_outline_rounded,
                      size : 16,
                      color: isSelected
                          ? _AppColors.primary
                          : _AppColors.drawerText,
                    ),
                    title: Text(
                      _sessions[index]["title"],
                      maxLines : 1,
                      overflow : TextOverflow.ellipsis,
                      style: TextStyle(
                        color     : isSelected
                            ? _AppColors.drawerTextBold
                            : _AppColors.drawerText,
                        fontSize  : 13.5,
                        fontWeight: isSelected
                            ? FontWeight.w600
                            : FontWeight.w400,
                      ),
                    ),
                    trailing: isSelected
                        ? GestureDetector(
                      onTap: () => _deleteSession(index),
                      child: const Icon(
                          Icons.delete_outline_rounded,
                          size : 16,
                          color: Colors.yellowAccent),
                    )
                        : null,
                    shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(10)),
                    onTap: () {
                      setState(() {
                        _currentIndex = index;
                        _isFirstOpen  = false;
                      });
                      Navigator.pop(context);
                    },
                  ),
                );
              },
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildInputBar() {
    return Container(
      padding: EdgeInsets.only(
        left  : 12,
        right : 12,
        top   : 10,
        bottom: MediaQuery.of(context).padding.bottom + 10,
      ),
      decoration: BoxDecoration(
        color     : _AppColors.surface,
        boxShadow : [
          BoxShadow(
            color     : Colors.black.withOpacity(0.07),
            blurRadius: 12,
            offset    : const Offset(0, -3),
          ),
        ],
      ),
      child: Row(
        children: [
          Expanded(
            child: GestureDetector(
              onTap: _isFirstOpen ? _handleFirstOpenTap : null,
              child: TextField(
                controller     : _controller,
                focusNode      : _inputFocusNode,
                enabled        : !_isLoading,
                maxLines       : 4,
                minLines       : 1,
                textInputAction: TextInputAction.send,
                onSubmitted    : _isLoading ? null : sendMessage,
                style: const TextStyle(
                    fontSize: 15, color: _AppColors.textPrimary),
                decoration: InputDecoration(
                  hintText: _isLoading
                      ? "Please wait…"
                      : "e.g. TR_3PH_CS_01_9V_4000A stage 1.3",
                  hintStyle: const TextStyle(
                      color: _AppColors.textLight, fontSize: 13.5),
                  filled   : true,
                  fillColor: _AppColors.background,
                  border: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(24),
                    borderSide  : BorderSide.none,
                  ),
                  enabledBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(24),
                    borderSide  : const BorderSide(
                        color: _AppColors.border, width: 1),
                  ),
                  focusedBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(24),
                    borderSide  : const BorderSide(
                        color: _AppColors.primary, width: 1.5),
                  ),
                  disabledBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(24),
                    borderSide  : const BorderSide(
                        color: _AppColors.border, width: 1),
                  ),
                  contentPadding: const EdgeInsets.symmetric(
                      horizontal: 18, vertical: 12),
                ),
              ),
            ),
          ),
          const SizedBox(width: 8),
          AnimatedContainer(
            duration: const Duration(milliseconds: 200),
            width : 44,
            height: 44,
            decoration: BoxDecoration(
              color        : _isLoading ? _AppColors.border : _AppColors.primary,
              borderRadius : BorderRadius.circular(22),
              boxShadow    : _isLoading
                  ? []
                  : [
                BoxShadow(
                  color     : _AppColors.primary.withOpacity(0.35),
                  blurRadius: 8,
                  offset    : const Offset(0, 3),
                ),
              ],
            ),
            child: IconButton(
              padding: EdgeInsets.zero,
              icon: _isLoading
                  ? const SizedBox(
                width : 18,
                height: 18,
                child : CircularProgressIndicator(
                  strokeWidth: 2,
                  color      : _AppColors.textLight,
                ),
              )
                  : const Icon(Icons.arrow_upward_rounded,
                  color: Colors.white, size: 20),
              onPressed: _isLoading
                  ? null
                  : () => sendMessage(_controller.text),
            ),
          ),
        ],
      ),
    );
  }
}
class _TypingDot extends StatefulWidget {
  final int delay;
  const _TypingDot({required this.delay});

  @override
  State<_TypingDot> createState() => _TypingDotState();
}

class _TypingDotState extends State<_TypingDot>
    with SingleTickerProviderStateMixin {
  late AnimationController _controller;
  late Animation<double>   _animation;

  @override
  void initState() {
    super.initState();
    _controller = AnimationController(
        vsync   : this,
        duration: const Duration(milliseconds: 550));
    _animation = Tween<double>(begin: 0.25, end: 1.0).animate(
        CurvedAnimation(parent: _controller, curve: Curves.easeInOut));
    Future.delayed(Duration(milliseconds: widget.delay), () {
      if (mounted) _controller.repeat(reverse: true);
    });
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return FadeTransition(
      opacity: _animation,
      child: Container(
        width : 8,
        height: 8,
        decoration: const BoxDecoration(
          color: _AppColors.primary,
          shape: BoxShape.circle,
        ),
      ),
    );
  }
}
