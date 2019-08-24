using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using OneWorldDbClient.SampleWeb.Data;
using OneWorldDbClient.SampleWeb.Models;
using OneWorldDbClient.SampleWeb.SampleDi;

namespace OneWorldDbClient.SampleWeb.Controllers
{
    public class HomeController : Controller
    {
        private readonly SampleDiLogicA _sampleDiLogicA;
        private readonly SampleDiLogicB _sampleDiLogicB;
        private readonly SampleDiLogicC _sampleDiLogicC;
        private readonly SampleDiLogicD _sampleDiLogicD;
        private readonly OneWorldDbClientManager<ApplicationDbContext> _dbManager;

        private readonly ILogger<HomeController> _logger;

        public HomeController(
            SampleDiLogicA sampleDiLogicA,
            SampleDiLogicB sampleDiLogicB,
            SampleDiLogicC sampleDiLogicC,
            SampleDiLogicD sampleDiLogicD,
            OneWorldDbClientManager<ApplicationDbContext> dbManager,
            ILogger<HomeController> logger)
        {
            _sampleDiLogicA = sampleDiLogicA;
            _sampleDiLogicB = sampleDiLogicB;
            _sampleDiLogicC = sampleDiLogicC;
            _sampleDiLogicD = sampleDiLogicD;
            _dbManager = dbManager;

            _logger = logger;
        }

        public async Task<IActionResult> Index()
        {

            var xA = await _sampleDiLogicA.SampleMethodAsync();
            // xA = 2

            var xB = await _sampleDiLogicB.SampleMethodAsync(xA);
            // xB = 4

            var xC = await _sampleDiLogicC.SampleMethodAsync();
            // xC = 0

            var xD = await _sampleDiLogicD.SampleMethodAsync();
            // xD = 4


            // It's also possible to tx status check with in the Controller.
            using (var txScope = await _dbManager.BeginTranRequiredAsync())
            {
                _logger.LogInformation($"txScope.Committable={txScope.Committable}");

                // if(!txScope.Committable)
                    // some tasks...

                txScope.VoteCommit();
                // An explicit declaration is required.
                // Not specifying is the same as `.VoteRollback()`.
            }

            // In this sample, it is rolled back at the end.

            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
