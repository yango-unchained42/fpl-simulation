# Sprint 3 - Rolling Features Engineering

## Sprint Goal
Implement rolling average features to capture player and team form over time windows.

## Duration
Week 4

## Team Capacity
- Project Owner: 2 hours
- Senior Data Engineer/Scientist: 20 hours
- Reviewer: 4 hours
- Test Specialist: 4 hours

## Success Criteria
- [ ] Rolling average features implemented using polars
- [ ] Multiple time windows supported (3, 5, 10 games)
- [ ] Rolling features stored in Supabase
- [ ] All rolling features tested
- [ ] Performance optimized for large datasets
- [ ] MLflow logging (local only)

## Risks & Blockers
- [ ] Performance issues with large datasets - Mitigation: Use vectorized operations and database indexing
- [ ] Edge cases at season start - Mitigation: Handle partial windows appropriately

---

## Tickets

### SPRINT3-001: Player Rolling Features
### SPRINT3-002: Team Rolling Features
### SPRINT3-003: Rolling Features Testing
