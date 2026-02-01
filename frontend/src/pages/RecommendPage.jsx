import StrategyBanner from "../components/recommend/StrategyBanner/StrategyBanner";
import TodayRecommend from "../components/recommend/TodayRecommend/TodayRecommend";

function RecommendPage() {
  return (
    <main className="page">
      <StrategyBanner />
      <TodayRecommend />
    </main>
  );
}

export default RecommendPage;
