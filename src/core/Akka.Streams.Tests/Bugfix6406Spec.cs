using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Tests
{
    public class Bugfix6406Spec : AkkaSpec
    {
        private ActorMaterializer Materializer { get; }

        public Bugfix6406Spec(ITestOutputHelper helper) : base(helper)
        {
            Materializer = ActorMaterializer.Create(Sys);
        }
        [Fact]
        public async Task FailingTest()
        {
            //USERS: THIS IS NOT RIGHT?? 
            var src = Source.From(new List<int> { 1, 2, 3, 4, 5 })
                .Throttle(
                    cost: 1,
                    per: TimeSpan.FromSeconds(1),
                    maximumBurst: 10,
                    calculateCost: e => e % 2 == 0 ? throw new Exception() : 1,
                    //calculateCost: e => e % 2,
                    mode: ThrottleMode.Shaping)
                .WithAttributes(new Attributes(new ActorAttributes.SupervisionStrategy(Deciders.ResumingDecider)));

            var result = await src.RunWith(Sink.Seq<int>(), Materializer);

            Assert.Equal(3, result.Count);
        }
        [Fact]
        public async Task WorkingTest()
        {
            //USERS: THIS IS RIGHT?? 
            var src = Source.From(new List<int> { 1, 2, 3, 4, 5 })
                          .Select(e => e % 2 == 0 ? throw new Exception() : (e, 1))
                          .Throttle(
                              cost: 1,
                              per: TimeSpan.FromSeconds(1),
                              maximumBurst: 10,
                              calculateCost: e => e.Item2,
                              mode: ThrottleMode.Shaping)
                          .Select(v => v.e)
                          .WithAttributes(new Attributes(new ActorAttributes.SupervisionStrategy(Deciders.ResumingDecider)));

            var result = await src.RunWith(Sink.Seq<int>(), Materializer);

            Assert.Equal(3, result.Count);
        }

        [Fact]
        public async Task WorkingTest2()
        {
            var src = Source.From(Three(new List<int> { 1, 2, 3, 4, 5 }))
               .Throttle(
                   cost: 1,
                   per: TimeSpan.FromSeconds(1),
                   maximumBurst: 10,
                   calculateCost: e => e,
                   mode: ThrottleMode.Shaping)
               .WithAttributes(new Attributes(new ActorAttributes.SupervisionStrategy(Deciders.ResumingDecider)));

            var result = await src.RunWith(Sink.Seq<int>(), Materializer);

            Assert.Equal(3, result.Count);
        }
        private List<int> Three(List<int> s)
        {
            var result = new List<int>(); 
            foreach (var i in s )
            {
                if(!(i % 2 == 0))
                    result.Add(i);  
            }
            return result;
        }
    }
}
