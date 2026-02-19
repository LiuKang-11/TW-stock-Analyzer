import StrategyBanner from "../components/recommend/StrategyBanner/StrategyBanner";
import TodayRecommend from "../components/recommend/TodayRecommend/TodayRecommend";

function RecommendPage() {
  return (
    <main className="page">
      <section className="strategy-banner">
        <div className="container">
          <StrategyBanner />
        </div>
      </section>

      <section className="today-recommend">
        <div className="container">
          <TodayRecommend />
        </div>
      </section>
    </main>
  );
}

export default RecommendPage;
