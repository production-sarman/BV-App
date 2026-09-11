import 'dart:convert';
import 'package:http/http.dart' as http;

class ApiService {
  static const String _configUrl =
      "https://api.github.com/repos/production-sarman/BV-App/contents/config.json";

  static String? _baseUrl;

  static Future<String> _getBaseUrl() async {
    final timestamp = DateTime.now().millisecondsSinceEpoch;

    final response = await http.get(
      Uri.parse("$_configUrl?t=$timestamp"),
      headers: {
        "Accept": "application/vnd.github.v3.raw",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
      },
    ).timeout(const Duration(seconds: 10));

    if (response.statusCode == 200) {
      final config = jsonDecode(response.body);
      _baseUrl = (config["base_url"] as String).trim();
      return _baseUrl!;
    } else {
      throw Exception("Failed to load config: ${response.statusCode}");
    }
  }


  static Future<Map<String, dynamic>> sendMessage(String message) async {
    try {
      final baseUrl = await _getBaseUrl();
      final url = Uri.parse("$baseUrl/vsm/query");

      final response = await http.post(
        url,
        headers: {"Content-Type": "application/json"},
        body: jsonEncode({"query": message}),
      ).timeout(const Duration(seconds: 120));

      if (response.statusCode == 200) {
        final contentType = response.headers['content-type'] ?? "";


        if (contentType.contains('application/zip')) {
          return {
            "is_zip": true,
            "bytes": response.bodyBytes,
            "filename": "download.zip",
            "answer": "Downloading Stage 2.4 file...",
            "found": true,
          };
        }

        final data = jsonDecode(response.body);

        final downloadUrl = (data["download_url"] ?? "").toString();
        final previewUrl  = (data["preview_url"] ?? "").toString();

        final fullDownload =
        downloadUrl.isNotEmpty ? "$baseUrl$downloadUrl" : "";

        final fullPreview =
        previewUrl.isNotEmpty ? "$baseUrl$previewUrl" : "";

        return {
          "is_zip": false,
          "answer":
          "Stage ${data["stage"] ?? ""} — ${data["product_code"] ?? ""}",
          "document": fullDownload,
          "preview_url": fullPreview,
          "explanation": data["explanation"] ?? "",
          "found": downloadUrl.isNotEmpty,
        };
      } else {
        throw Exception(
            "Query failed: ${response.statusCode} ${response.body}");
      }
    } catch (e) {
      throw Exception("Network error: $e");
    }
  }
}