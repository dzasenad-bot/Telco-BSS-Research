# Telco BSS Market Research Agent (Operator-centric + Competitor-aware)
Stack: n8n/Make + Python + Postgres + Qdrant + WordPress + CRM

Start here:
1) Review 00-admin/charter.md & roadmap.md
2) Seed operators in 06-delivery/briefs/operator-centric/*
3) Use 02-sources/seed_lists/news_rss.csv and vendors/watchlists/*
4) Configure 04-pipelines/configs/*.yaml and .env

Daily outputs:
- Operator Daily Briefs per operator
- Competitor Weekly with top moves
- Battlecards refresh when enough signals
