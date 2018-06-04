using Microsoft.EntityFrameworkCore;

using NuClear.Broadway.Interfaces.Models;

namespace NuClear.Broadway.DataProjection
{
    public class DataProjectionContext : DbContext
    {
        public DbSet<Category> Categories { get; set; }
        public DbSet<SecondRubric> SecondRubrics { get; set; }
        public DbSet<Rubric> Rubrics { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var category = modelBuilder.Entity<Category>();
            var secondRubric = modelBuilder.Entity<SecondRubric>();
            var rubric = modelBuilder.Entity<Rubric>();

            category.HasKey(x => x.Code);
            category.Property(x => x.IsDeleted).IsRequired();

            secondRubric.HasKey(x => x.Code);
            secondRubric.Property(x => x.IsDeleted).IsRequired();

            rubric.HasKey(x => x.Code);
            rubric.Property(x => x.Code).IsRequired();
        }
    }
}