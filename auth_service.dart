import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:shared_preferences/shared_preferences.dart';

class AuthService {
  static const String _configUrl =
      "https://api.github.com/repos/production-sarman/BV-App/contents/config.json";

  static String? _baseUrl;

  static Future<String> _getBaseUrl() async {
    final timestamp = DateTime.now().millisecondsSinceEpoch;

    print(">>> Fetching config from GitHub API...");
    final response = await http
        .get(
      Uri.parse("$_configUrl?t=$timestamp"),
      headers: {
        "Accept"       : "application/vnd.github.v3.raw",
        "Cache-Control": "no-cache",
        "Pragma"       : "no-cache",
      },
    )
        .timeout(const Duration(seconds: 10));

    if (response.statusCode == 200) {
      final config = jsonDecode(response.body);
      _baseUrl = (config["base_url"] as String).trim();
      print(">>> Base URL loaded: $_baseUrl");
      return _baseUrl!;
    } else {
      throw Exception("Failed to load config: ${response.statusCode}");
    }
  }

  static Future<Map<String, dynamic>> login(
      String username, String password) async {
    try {
      _baseUrl = null;
      final baseUrl = await _getBaseUrl();
      final url = Uri.parse("$baseUrl/auth/login");

      print(">>> Sending login request to $url");

      final response = await http.post(
        url,
        headers: {"Content-Type": "application/json"},
        body: jsonEncode({"username": username, "password": password}),
      ).timeout(const Duration(seconds: 10));
      print(">>> Status: ${response.statusCode}");
      print(">>> Body: ${response.body}");

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body);
        final token = data["access_token"];
        if (token != null) {
          final prefs = await SharedPreferences.getInstance();
          await prefs.setString('access_token', token);
          print(">>> Token saved");
        }
        return {"success": true, "token": token};
      } else {
        String errorMsg = "Login failed";
        try {
          errorMsg = jsonDecode(response.body)["detail"] ?? errorMsg;
        } catch (_) {}
        return {"success": false, "error": errorMsg};
      }
    } catch (e) {
      print(">>> ERROR: $e");
      return {"success": false, "error": "Network error: $e"};
    }
  }

  static Future<Map<String, dynamic>> register(
      String username, String password) async {
    try {
      _baseUrl = null;
      final baseUrl = await _getBaseUrl();
      final url = Uri.parse("$baseUrl/auth/register");

      print(">>> Sending register request to $url");

      final response = await http.post(
        url,
        headers: {"Content-Type": "application/json"},
        body: jsonEncode({"username": username, "password": password}),
      ).timeout(const Duration(seconds: 10));

      print(">>> Status: ${response.statusCode}");
      print(">>> Body: ${response.body}");

      if (response.statusCode == 200) {
        return {"success": true};
      } else {
        String errorMsg = "Registration failed";
        try {
          errorMsg = jsonDecode(response.body)["detail"] ?? errorMsg;
        } catch (_) {}
        return {"success": false, "error": errorMsg};
      }
    } catch (e) {
      print(">>> ERROR: $e");
      return {"success": false, "error": "Network error: $e"};
    }
  }

  static Future<String?> getToken() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString('access_token');
  }

  static Future<void> logout() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove('access_token');
    _baseUrl = null;
    print(">>> Logged out");
  }
}