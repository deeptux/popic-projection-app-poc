import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';

@Component({
  selector: 'app-dashboard-page',
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: './dashboard-page.html',
  styleUrls: ['./dashboard-page.css']
})
export class DashboardPage {
  // Mock Data for Summary Cards
  summaryCards = [
    { title: 'RLIP', value: '$2,464', change: 'Last Month', percent: 70, icon: 'tag', color: 'text-blue-500', barColor: 'bg-orange-500' },
    { title: 'RAP', value: '$2,464', change: 'Last Month', percent: 50, icon: 'bag', color: 'text-blue-500', barColor: 'bg-orange-500' },
    { title: 'STORAGE', value: '$246', change: 'Last Month', percent: 30, icon: 'refresh', color: 'text-orange-500', barColor: 'bg-orange-500' }
  ];

  // Mock Data for Sales List (Right Sidebar)
  salesList = [
    { name: 'Jana Lloyd', role: 'Sales', sales: '$1256', profit: '$337', avatar: 'https://i.pravatar.cc/150?u=1' },
    { name: 'Chris Wall', role: 'Sales', sales: '$1256', profit: '$789', avatar: 'https://i.pravatar.cc/150?u=2' },
    { name: 'Morgana Jensen', role: 'Sales', sales: '$1256', profit: '$459', avatar: 'https://i.pravatar.cc/150?u=3' },
  ];

  // Mock Data for Bar Chart
  barChartData = [60, 80, 40, 90, 50]; // Heights in percentage
}