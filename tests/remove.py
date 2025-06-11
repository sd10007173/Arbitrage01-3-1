#!/usr/bin/env python3
"""
CSV Data Cleanup Utility
========================

A comprehensive tool for safely removing CSV files from the data directory.
Includes safety confirmations, progress tracking, and multiple operation modes.

Usage:
    python tests/remove.py              # Interactive mode with confirmation
    python tests/remove.py --force      # Skip confirmation prompts
    python tests/remove.py --stats      # Show statistics only (no deletion)
    python tests/remove.py --gitkeep    # Create .gitkeep files after cleanup
"""

import os
import sys
import argparse
import glob
from pathlib import Path
from typing import List, Dict, Tuple
import time

class CSVCleaner:
    """Comprehensive CSV data cleaning utility"""
    
    def __init__(self, base_dir: str = None):
        """Initialize the CSV cleaner with base directory"""
        if base_dir is None:
            # Auto-detect base directory (assume script is in tests/ subdirectory)
            script_dir = Path(__file__).parent
            self.base_dir = script_dir.parent
        else:
            self.base_dir = Path(base_dir)
        
        self.csv_dir = self.base_dir / "csv"
        self.stats = {
            'files_found': 0,
            'files_deleted': 0,
            'directories_processed': 0,
            'total_size_mb': 0.0,
            'errors': []
        }
    
    def scan_csv_files(self) -> Dict[str, List[Path]]:
        """Scan and categorize all CSV files in the directory"""
        print(f"🔍 Scanning CSV directory: {self.csv_dir}")
        
        if not self.csv_dir.exists():
            print(f"❌ CSV directory not found: {self.csv_dir}")
            return {}
        
        file_categories = {
            'funding_rates': [],
            'return_analysis': [],
            'strategy_ranking': [],
            'backtest': [],
            'other': []
        }
        
        # Use glob patterns to find CSV files
        csv_pattern = str(self.csv_dir / "**" / "*.csv")
        csv_files = glob.glob(csv_pattern, recursive=True)
        
        for file_path in csv_files:
            path_obj = Path(file_path)
            file_name = path_obj.name.lower()
            parent_dir = path_obj.parent.name.lower()
            
            # Categorize files based on naming patterns and parent directory
            if any(keyword in file_name for keyword in ['funding', 'fr_diff', 'fr_history']) or \
               any(keyword in parent_dir for keyword in ['fr_diff', 'fr_history']):
                file_categories['funding_rates'].append(path_obj)
            elif any(keyword in file_name for keyword in ['fr_return_list', 'return_list']) or \
                 'fr_return_list' in parent_dir:
                file_categories['return_analysis'].append(path_obj)
            elif any(keyword in file_name for keyword in ['ranking']) or \
                 'strategy_ranking' in parent_dir:
                file_categories['strategy_ranking'].append(path_obj)
            elif any(keyword in file_name for keyword in ['backtest', 'bt_', 'test']) or \
                 'backtest' in parent_dir:
                file_categories['backtest'].append(path_obj)
            else:
                file_categories['other'].append(path_obj)
        
        # Calculate total files and size
        total_files = sum(len(files) for files in file_categories.values())
        total_size = 0
        
        for files in file_categories.values():
            for file_path in files:
                try:
                    total_size += file_path.stat().st_size
                except OSError:
                    pass
        
        self.stats['files_found'] = total_files
        self.stats['total_size_mb'] = total_size / (1024 * 1024)
        
        return file_categories
    
    def display_statistics(self, file_categories: Dict[str, List[Path]]) -> None:
        """Display detailed statistics about found files"""
        print("\n" + "="*60)
        print("📊 CSV FILES STATISTICS")
        print("="*60)
        
        if not any(file_categories.values()):
            print("✅ No CSV files found in the directory.")
            return
        
        for category, files in file_categories.items():
            if files:
                print(f"\n📁 {category.upper().replace('_', ' ')}:")
                print(f"   Count: {len(files)} files")
                
                # Calculate category size
                category_size = 0
                for file_path in files:
                    try:
                        category_size += file_path.stat().st_size
                    except OSError:
                        pass
                
                print(f"   Size:  {category_size / (1024 * 1024):.2f} MB")
                
                # Show sample files (first 3)
                print("   Files:")
                for i, file_path in enumerate(files[:3]):
                    rel_path = file_path.relative_to(self.csv_dir)
                    print(f"     • {rel_path}")
                
                if len(files) > 3:
                    print(f"     ... and {len(files) - 3} more files")
        
        print(f"\n🎯 TOTAL SUMMARY:")
        print(f"   Files: {self.stats['files_found']} CSV files")
        print(f"   Size:  {self.stats['total_size_mb']:.2f} MB")
        print("="*60)
    
    def confirm_deletion(self, file_categories: Dict[str, List[Path]]) -> bool:
        """Get user confirmation for deletion"""
        if not any(file_categories.values()):
            return False
        
        print(f"\n⚠️  WARNING: This will permanently delete {self.stats['files_found']} CSV files")
        print(f"   Total size: {self.stats['total_size_mb']:.2f} MB")
        print("   This action cannot be undone!")
        
        while True:
            response = input("\n❓ Do you want to proceed? (yes/no): ").lower().strip()
            if response in ['yes', 'y']:
                return True
            elif response in ['no', 'n']:
                print("❌ Operation cancelled by user.")
                return False
            else:
                print("Please enter 'yes' or 'no'")
    
    def delete_files(self, file_categories: Dict[str, List[Path]], show_progress: bool = True) -> None:
        """Delete all CSV files with progress tracking"""
        all_files = []
        for files in file_categories.values():
            all_files.extend(files)
        
        if not all_files:
            print("✅ No files to delete.")
            return
        
        print(f"\n🗑️  Deleting {len(all_files)} CSV files...")
        
        for i, file_path in enumerate(all_files, 1):
            try:
                if show_progress and i % 10 == 0:
                    progress = (i / len(all_files)) * 100
                    print(f"   Progress: {progress:.1f}% ({i}/{len(all_files)})")
                
                file_path.unlink()
                self.stats['files_deleted'] += 1
                
            except OSError as e:
                error_msg = f"Failed to delete {file_path}: {e}"
                self.stats['errors'].append(error_msg)
                print(f"❌ {error_msg}")
        
        print(f"✅ Deleted {self.stats['files_deleted']} files")
        
        if self.stats['errors']:
            print(f"⚠️  {len(self.stats['errors'])} errors occurred during deletion")
    
    def create_gitkeep_files(self) -> None:
        """Create .gitkeep files to preserve directory structure"""
        print("\n📁 Creating .gitkeep files...")
        
        # Find all subdirectories in csv/
        if not self.csv_dir.exists():
            return
        
        gitkeep_created = 0
        for root, dirs, files in os.walk(self.csv_dir):
            root_path = Path(root)
            
            # Check if directory is empty (no files, only subdirectories)
            has_files = any(Path(root, f).is_file() for f in files if not f.startswith('.'))
            
            if not has_files:
                gitkeep_path = root_path / ".gitkeep"
                if not gitkeep_path.exists():
                    try:
                        gitkeep_path.touch()
                        gitkeep_created += 1
                        rel_path = root_path.relative_to(self.csv_dir)
                        print(f"   Created: {rel_path}/.gitkeep")
                    except OSError as e:
                        print(f"❌ Failed to create .gitkeep in {root_path}: {e}")
        
        if gitkeep_created > 0:
            print(f"✅ Created {gitkeep_created} .gitkeep files")
        else:
            print("ℹ️  No .gitkeep files needed")
    
    def run_cleanup(self, force: bool = False, stats_only: bool = False, create_gitkeep: bool = False) -> None:
        """Main cleanup execution method"""
        print("🧹 CSV Data Cleanup Utility")
        print("="*40)
        
        # Scan files
        file_categories = self.scan_csv_files()
        
        # Display statistics
        self.display_statistics(file_categories)
        
        # If stats only, return early
        if stats_only:
            print("\n📋 Statistics displayed. No files were deleted.")
            return
        
        # Check if there are files to delete
        if not any(file_categories.values()):
            print("\n✅ Directory is already clean!")
            return
        
        # Get confirmation (unless forced)
        if not force:
            if not self.confirm_deletion(file_categories):
                return
        
        # Delete files
        start_time = time.time()
        self.delete_files(file_categories)
        deletion_time = time.time() - start_time
        
        # Create .gitkeep files if requested
        if create_gitkeep:
            self.create_gitkeep_files()
        
        # Final summary
        print(f"\n🎉 CLEANUP COMPLETED")
        print(f"   Files deleted: {self.stats['files_deleted']}")
        print(f"   Space freed:   {self.stats['total_size_mb']:.2f} MB")
        print(f"   Time taken:    {deletion_time:.2f} seconds")
        
        if self.stats['errors']:
            print(f"   Errors:        {len(self.stats['errors'])}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="CSV Data Cleanup Utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tests/remove.py              # Interactive cleanup
  python tests/remove.py --force      # Skip confirmations
  python tests/remove.py --stats      # Show statistics only
  python tests/remove.py --gitkeep    # Create .gitkeep files after cleanup
        """
    )
    
    parser.add_argument(
        '--force', 
        action='store_true',
        help='Skip confirmation prompts and delete immediately'
    )
    
    parser.add_argument(
        '--stats', 
        action='store_true',
        help='Show file statistics only (no deletion)'
    )
    
    parser.add_argument(
        '--gitkeep', 
        action='store_true',
        help='Create .gitkeep files in empty directories after cleanup'
    )
    
    parser.add_argument(
        '--dir',
        type=str,
        default=None,
        help='Specify custom base directory (default: auto-detect)'
    )
    
    args = parser.parse_args()
    
    try:
        cleaner = CSVCleaner(base_dir=args.dir)
        cleaner.run_cleanup(
            force=args.force,
            stats_only=args.stats,
            create_gitkeep=args.gitkeep
        )
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation interrupted by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 