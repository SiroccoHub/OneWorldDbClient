using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.UI;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.EntityFrameworkCore;
using OneWorldDbClient.SampleWeb.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OneWorldDbClient.SampleWeb.SampleDi;

namespace OneWorldDbClient.SampleWeb
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddDbContext<ApplicationDbContext>(options =>
                options.UseSqlServer(
                    Configuration.GetConnectionString("DefaultConnection")));

            services.AddDefaultIdentity<IdentityUser>(options => options.SignIn.RequireConfirmedAccount = true)
                .AddEntityFrameworkStores<ApplicationDbContext>();

            services.AddControllersWithViews();
            services.AddRazorPages();


            services.AddScoped(_ =>
                new OneWorldDbClientManager<ApplicationDbContext>(
                    Configuration.GetConnectionString("DefaultConnection"),
                    (connection, transaction) =>
                    {
                        var dbContext = new ApplicationDbContext(
                            new DbContextOptionsBuilder<ApplicationDbContext>()
                            .UseSqlServer((DbConnection)connection)
                            .Options);
                        dbContext.Database.UseTransaction((SqlTransaction)transaction);
                        return dbContext;
                    },
                    connStr => new SqlConnection(connStr),
                    _.GetRequiredService<ILogger<OneWorldDbClientManager<ApplicationDbContext>>>(),
                    _.GetRequiredService<ILogger<OneWorldDbTransaction<ApplicationDbContext>>>()));

            services.AddScoped<SampleDiLogicA>();
            services.AddScoped<SampleDiLogicB>();
            services.AddScoped<SampleDiLogicC>();
            services.AddScoped<SampleDiLogicD>();

        }


        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseDatabaseErrorPage();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }
            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseRouting();

            app.UseAuthentication();
            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}");
                endpoints.MapRazorPages();
            });
        }
    }
}
