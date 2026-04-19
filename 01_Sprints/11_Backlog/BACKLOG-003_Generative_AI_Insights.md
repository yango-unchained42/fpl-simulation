# Ticket: BACKLOG-003 - Generative AI Insights & Recommendations

## Description
Add generative AI capabilities to provide natural language insights, transfer recommendations, and player analysis summaries. This transforms the system from purely predictive to an interactive assistant.

## Technical Requirements
- Integrate LLM (OpenAI GPT, Anthropic Claude, or open-source like Ollama)
- Create prompts that synthesize prediction data into readable insights
- Generate transfer recommendations with reasoning
- Add sentiment analysis on news/injury reports
- Create match preview narratives
- Cache responses to avoid repeated API calls and costs

## Acceptance Criteria
- [ ] LLM integration working (any of: GPT, Claude, Ollama)
- [ ] Player performance insights generated in natural language
- [ ] Transfer recommendations include reasoning
- [ ] News sentiment analysis implemented
- [ ] Match preview narratives generated

## Definition of Done
- [ ] Code implemented and follows project conventions
- [ ] Unit tests written and passing
- [ ] Type hints added for public APIs
- [ ] Integrated into Streamlit app

## Agent
[Unassigned]

## Status
📋 Backlog

## Priority
Nice to Have (Enhancement)

## Progress Log

## Comments
- Depends on: SPRINT9-002 (Player Projections), SPRINT9-003 (Team Optimizer)
- Consider using @st.cache_data for LLM responses
- Budget consideration: open-source Ollama could run locally for free
