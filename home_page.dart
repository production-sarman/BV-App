import 'package:flutter/material.dart';
import 'support_page.dart';
import 'VSM_Assistant.dart';
import 'attendance_page.dart';

class HomePage extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Tools Dashboard'),
        centerTitle: true
      ),
      body: Padding(
        padding:  const EdgeInsets.all(16),
        child: GridView.count(
          crossAxisCount: 3, // number of icons per row
          crossAxisSpacing: 10, //It's Widget Section depth
          mainAxisSpacing: 10,
          children: [
            DashboardTile(
              title: 'Support',
              icon: Icons.support_agent,
              onTap: () {
                Navigator.push(
                  context,
                  MaterialPageRoute(builder: (_) => SupportPage()),
                );
              },
            ),
            DashboardTile(
              title: 'VSM Assistant',
              icon: Icons.assistant,
              onTap: () {
                Navigator.push(
                  context,
                  MaterialPageRoute(builder: (_) => VSM_Assistant()),
                );
              },
            ),
            DashboardTile(
              title: 'Attendance',
              icon: Icons.fact_check,
              onTap: () {
                Navigator.push(
                  context,
                  MaterialPageRoute(builder: (_) => AttendancePage()),
                );
              },
            ),
          ],
        ),
      ),
    );
  }
}

class DashboardTile extends StatelessWidget {
  final String title;
  final IconData icon;
  final VoidCallback onTap;
  const DashboardTile({
    required this.title,
    required this.icon,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return InkWell(
      borderRadius: BorderRadius.circular(6),
      onTap: onTap,
      child: Container(
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(6),
          boxShadow: [
            BoxShadow(
              color: Colors.black12,
              blurRadius: 6,
              offset: Offset(0, 3),
            ),
          ],
        ),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(icon, size: 40, color: Colors.blue),
            SizedBox(height: 10),
            Text(
              title,
              style: TextStyle(fontWeight: FontWeight.w600),
            ),
          ],
        ),
      ),
    );
  }
}
