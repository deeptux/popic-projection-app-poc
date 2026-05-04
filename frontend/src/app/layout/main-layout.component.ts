import { Component, inject, OnInit, OnDestroy } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { BreakpointObserver } from '@angular/cdk/layout';
import { SidenavComponent } from '../shared/sidenav';
import { SidenavService } from '../shared/sidenav/sidenav.service';

@Component({
  selector: 'app-main-layout',
  standalone: true,
  imports: [RouterOutlet, SidenavComponent],
  templateUrl: './main-layout.component.html',
  styleUrl: './main-layout.component.css',
})
export class MainLayoutComponent implements OnInit, OnDestroy {
  private readonly breakpointObserver = inject(BreakpointObserver);
  readonly sidenav = inject(SidenavService);
  isDesktop = false;
  private sub: { unsubscribe: () => void } | null = null;

  ngOnInit() {
    this.sub = this.breakpointObserver.observe('(min-width: 1024px)').subscribe(state => {
      this.isDesktop = state.matches;
    });
  }

  ngOnDestroy() {
    this.sub?.unsubscribe();
  }
}
