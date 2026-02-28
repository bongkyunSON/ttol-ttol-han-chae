```mermaid
flowchart LR
  A[Phase 1<br/>Data Pipeline<br/>(부동산원 + 국토부)] --> B[Phase 2<br/>Market Analysis Layer<br/>(주간동향/매수우위/거래)]
  B --> C[Phase 3<br/>Feature Engineering<br/>(단지/면적 지표)]
  C --> D[Phase 4<br/>Ranking & Scoring<br/>(서열/가중치)]
  D --> E[Phase 5<br/>Recommendation Layer<br/>(5억 추천 + 근거)]
  E --> F[Phase 6<br/>Expansion<br/>(정책/경매/포트폴리오)]
