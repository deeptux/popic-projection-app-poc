import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalyzePage } from './analyze-page';

describe('AnalyzePage', () => {
  let component: AnalyzePage;
  let fixture: ComponentFixture<AnalyzePage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AnalyzePage]
    }).compileComponents();

    fixture = TestBed.createComponent(AnalyzePage);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
