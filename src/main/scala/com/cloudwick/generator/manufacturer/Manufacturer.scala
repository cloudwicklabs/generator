package com.cloudwick.generator.manufacturer

import com.cloudwick.generator.utils.Utils

import scala.util.Random

class Manufacturer {
  val random = Random
  val utils = new Utils

  val TYPES = Map(
    "INC" -> 40,
    "LLC" -> 10,
    "CORP" -> 10,
    "NA" -> 40
  )

  val techTerms = Array(
    "AddOn", "Algorithm", "Architect", "Array", "Asynchronous", "Avatar", "Band", "Base", "Beta",
    "Binary", "Blog", "Board", "Boolean", "Boot", "Bot", "Browser", "Bug", "Cache", "Character",
    "Checksum", "Chip", "Circuit", "Client", "Cloud", "Cluster", "Code", "Codec", "Coder", "Column",
    "Command", "Compile", "Compression", "Computing", "Console", "Constant", "Control", "Cookie",
    "Core", "Cyber", "Default", "Deprecated", "Dev", "Developer", "Development", "Device",
    "Digital", "Domain", "Dynamic", "Emulation", "Encryption", "Engine", "Error", "Exception",
    "Exploit", "Export", "Extension", "File", "Font", "Fragment", "Frame", "Function", "Group",
    "Hacker", "Hard", "HTTP", "Icon", "Input", "IT", "Kernel", "Key", "Leak", "Link", "Load",
    "Logic", "Mail", "Mashup", "Mega", "Meme", "Memory", "Meta", "Mount", "Navigation", "Net",
    "Node", "Open", "OS", "Output", "Over", "Packet", "Page", "Parallel", "Parse", "Path", "Phone",
    "Ping", "Pixel", "Port", "Power", "Programmers", "Programs", "Protocol", "Push", "Query",
    "Queue", "Raw", "Real", "Repository", "Restore", "Root", "Router", "Run", "Safe", "Sample",
    "Scalable", "Script", "Server", "Session", "Shell", "Smart", "Socket", "Soft", "Solid", "Sound",
    "Source", "Streaming", "Symfony", "Syntax", "System", "Tag", "Tape", "Task", "Template",
    "Thread", "Token", "Tool", "Tweak", "URL", "Utility", "Viral", "Volume", "Ware", "Web", "Wiki",
    "Window", "Wire"
  )

  val culinaryTerms = Array(
    "Appetit", "Bake", "Beurre", "Bistro", "Blend", "Boil", "Bouchees", "Brew", "Buffet", "Caffe",
    "Caffeine", "Cake", "Carve", "Caviar", "Chef", "Chocolate", "Chop", "Citrus", "Cocoa",
    "Compote", "Cook", "Cooker", "Cookery", "Cool", "Core", "Coulis", "Course", "Crouton",
    "Cuisine", "Dash", "Dessert", "Dip", "Dish", "Dress", "Entree", "Espresso", "Extracts",
    "Fajitas", "Fibers", "Fold", "Formula", "Fruit", "Fumet", "Fusion", "Gastronomy", "Glucose",
    "Gourmet", "Grains", "Gratin", "Greens", "Guacamole", "Herbs", "Honey", "Hybrid", "Ice",
    "Icing", "Immersion", "Induction", "Instant", "Jasmine", "Jelly", "Juice", "Kiwi", "Lean",
    "Leek", "Legumes", "Lemon", "Lime", "Liqueur", "Madeleine", "Mango", "Marinate", "Melon",
    "Mill", "Mince", "Mirepoix", "Mix", "Mousse", "Muffin", "Mull", "Munster", "Nectar", "Nut",
    "Olive", "Organic", "Organic", "Pan", "Papillote", "Pare", "Pasta", "Pate", "Peanut", "Pear",
    "Pesto", "Picante", "Pie", "Pigment", "Pinot", "Plate", "Plum", "Pod", "Prepare", "Pressure",
    "Pudding", "Pulp", "Quiche", "Rack", "Raft", "Raisin", "Rape", "Recipe", "Reduce", "Relish",
    "Render", "Risotto", "Rosemary", "Roux", "Rub", "Salad", "Salsa", "Sauce", "Sauté", "Season",
    "Slice", "Smoked", "Soft", "Sorbet", "Soup", "Spaghetti", "Specialty", "Spicy", "Splash",
    "Steam", "Stem", "Sticky", "Stuff", "Sugar", "Supreme", "Sushi", "Sweet", "Table", "Tart",
    "Taste", "Tasting", "Tea", "Tender", "Terrine", "Tomato", "Vanilla", "Wash", "Wax", "Wine",
    "Wok", "Zest"
  )

  def techTerm() = {
    techTerms(random.nextInt(techTerms.length))
  }

  def culinaryTerm() = {
    culinaryTerms(random.nextInt(culinaryTerms.length))
  }

  def manufacturerNameFormats() = {
    Array(
      s"${techTerm()}${culinaryTerm()}",
      s"${techTerm()}${techTerm()}",
      s"${culinaryTerm()}${techTerm()}"
    )
  }

  def gen = {
    val manufacturerNameFormat = manufacturerNameFormats()
    val manufacturerName = manufacturerNameFormat(random.nextInt(manufacturerNameFormat.length))

    utils.pickWeightedKey(TYPES) match {
      case "INC" => s"$manufacturerName INC"
      case "LLC" => s"$manufacturerName LLC"
      case "CORP" => s"$manufacturerName CORP"
      case "NA" => s"$manufacturerName"
    }
  }
}
